//! Operational state-change events (`zebra.state.v1`).
//!
//! A process-wide hub that protocol tasks publish state transitions
//! to and `StateService.Subscribe` streams fan out from. It is a
//! global rather than a handle threaded through every spawn path
//! because publishers live in every protocol task and every per-VRF
//! instance of it.
//!
//! Publishing never blocks a protocol: the hub is a
//! `tokio::sync::broadcast` channel, and a subscriber that falls more
//! than [`CHANNEL_CAPACITY`] events behind is disconnected (it sees
//! `Lagged`) instead of applying backpressure.
//!
//! Events are only built while someone listens. Each family keeps a
//! count of the subscribers selecting it, and call sites gate on
//! [`wants`] — one relaxed atomic load — before constructing the
//! payload:
//!
//! ```ignore
//! if event::wants(EventKind::BgpPeer) {
//!     event::publish(vrf_id, vrf, Event::BgpPeer(…));
//! }
//! ```
//!
//! A subscriber can also ask for a snapshot of the current state.
//! Every protocol task that owns state registers a [`SnapshotSource`];
//! a snapshot request reaches each of them, and each answers from its
//! own event loop by publishing its objects into a channel private to
//! that request. Snapshot and live events take their `seq` from the
//! same counter under the same lock, so the subscriber merges the two
//! channels by `seq` and every object's history stays in order. The
//! private channel closes when the last source has answered, which is
//! how the subscriber knows the snapshot is complete.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, mpsc};

mod service;
pub use service::{StateServiceServer, StateSubscribeService};

pub mod pb {
    tonic::include_proto!("zebra.state.v1");
}

pub use pb::EventKind;
pub use pb::state_event::Event;

/// Events buffered per subscriber before it is declared lagging and
/// disconnected. Session-level transitions arrive at most a few
/// hundred per second even during a flap, so this only fills when a
/// client stops reading.
const CHANNEL_CAPACITY: usize = 1024;

/// Every selectable family, the expansion of an empty `kinds` list.
const ALL_KINDS: [EventKind; 6] = [
    EventKind::BgpPeer,
    EventKind::BfdSession,
    EventKind::OspfNeighbor,
    EventKind::Interface,
    EventKind::SpfRun,
    EventKind::IsisAdjacency,
];

/// One slot per `EventKind` discriminant (slot 0, UNSPECIFIED, unused).
const KIND_SLOTS: usize = ALL_KINDS.len() + 1;

static HUB: LazyLock<EventHub> = LazyLock::new(EventHub::new);

pub struct EventHub {
    /// Last sequence number handed out. Every send happens while this
    /// is held, so `seq` order is delivery order across publishing
    /// tasks and across the live and snapshot channels. Only taken
    /// while someone listens.
    seq: Mutex<u64>,
    tx: broadcast::Sender<pb::StateEvent>,
    /// Subscribers selecting each family, indexed by discriminant.
    interest: [AtomicUsize; KIND_SLOTS],
    /// Snapshot sources: one per running protocol task that owns state.
    sources: Mutex<HashMap<u64, mpsc::UnboundedSender<SnapshotRequest>>>,
    next_source: AtomicU64,
}

impl EventHub {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            seq: Mutex::new(0),
            tx,
            interest: Default::default(),
            sources: Mutex::new(HashMap::new()),
            next_source: AtomicU64::new(0),
        }
    }

    fn wants(&self, kind: EventKind) -> bool {
        self.interest[kind as usize].load(Ordering::Relaxed) > 0
    }

    /// Stamp `event` with the next `seq` and hand it to `send` while
    /// the sequence lock is held.
    fn stamp(&self, vrf_id: u32, vrf: String, event: Event, send: impl FnOnce(pb::StateEvent)) {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut seq = self.seq.lock().unwrap_or_else(|e| e.into_inner());
        *seq += 1;
        send(pb::StateEvent {
            seq: *seq,
            timestamp_ns,
            vrf_id,
            vrf,
            event: Some(event),
            ..Default::default()
        });
    }

    fn publish(&self, vrf_id: u32, vrf: String, event: Event) {
        // Err only means nobody is subscribed right now — the last
        // subscriber left between the caller's `wants` and here.
        self.stamp(vrf_id, vrf, event, |ev| {
            let _ = self.tx.send(ev);
        });
    }

    /// Register a subscriber for `kinds`. Interest is counted from
    /// here until the returned [`Interest`] is dropped.
    fn register(
        &'static self,
        kinds: &[EventKind],
    ) -> (Interest, broadcast::Receiver<pb::StateEvent>) {
        let rx = self.tx.subscribe();
        for kind in kinds {
            self.interest[*kind as usize].fetch_add(1, Ordering::Relaxed);
        }
        (
            Interest {
                hub: self,
                kinds: kinds.to_vec(),
            },
            rx,
        )
    }
}

impl EventHub {
    fn add_source(&'static self) -> SnapshotSource {
        let id = self.next_source.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, tx);
        SnapshotSource { hub: self, id, rx }
    }

    fn request_snapshot(
        &'static self,
        kinds: &[EventKind],
    ) -> mpsc::UnboundedReceiver<pb::StateEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        for source in self
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            // A source whose task is gone drops the request unanswered,
            // which releases its sender just as an answer would.
            let _ = source.send(SnapshotRequest {
                hub: self,
                kinds: kinds.to_vec(),
                tx: tx.clone(),
            });
        }
        // The caller's copy goes now, so `rx` closes exactly when the
        // last source has answered.
        rx
    }
}

/// One protocol task's registration for snapshot requests. Register
/// it when the task enters its main loop, poll [`Self::recv`] there,
/// and answer each request from the task's own state. Dropping it
/// (the task ending) unregisters.
pub struct SnapshotSource {
    hub: &'static EventHub,
    id: u64,
    rx: mpsc::UnboundedReceiver<SnapshotRequest>,
}

impl SnapshotSource {
    pub async fn recv(&mut self) -> Option<SnapshotRequest> {
        self.rx.recv().await
    }
}

impl Drop for SnapshotSource {
    fn drop(&mut self) {
        self.hub
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

/// A request to publish the current state of `kinds`. Dropping it
/// (after publishing) tells the requester this source is done.
pub struct SnapshotRequest {
    hub: &'static EventHub,
    kinds: Vec<EventKind>,
    tx: mpsc::UnboundedSender<pb::StateEvent>,
}

impl SnapshotRequest {
    /// Whether the requester selected `kind`; skip building the rest.
    pub fn wants(&self, kind: EventKind) -> bool {
        self.kinds.contains(&kind)
    }

    /// Publish one object's current state to the requester only.
    pub fn publish(&self, vrf_id: u32, vrf: String, event: Event) {
        self.hub.stamp(vrf_id, vrf, event, |mut ev| {
            ev.snapshot = true;
            let _ = self.tx.send(ev);
        });
    }
}

/// A subscriber's claim on its families; dropping it stops the daemon
/// building events nobody else wants.
pub struct Interest {
    hub: &'static EventHub,
    kinds: Vec<EventKind>,
}

impl Drop for Interest {
    fn drop(&mut self) {
        for kind in &self.kinds {
            self.hub.interest[*kind as usize].fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// A VRF identity for protocol state that outlives a single message
/// handler (e.g. an OSPF link), so publishers deep in a state machine
/// can tag events without reaching back to the instance's context.
#[derive(Clone, Debug, Default)]
pub struct VrfTag {
    pub id: u32,
    /// Empty for the default VRF.
    pub name: String,
}

impl VrfTag {
    pub fn of(ctx: &crate::context::ProtoContext) -> Self {
        Self {
            id: ctx.vrf_id(),
            name: ctx.vrf_ifname().unwrap_or_default().to_string(),
        }
    }
}

/// Whether any subscriber selects `kind`. Gate payload construction
/// on this; it is a single relaxed atomic load.
pub fn wants(kind: EventKind) -> bool {
    HUB.wants(kind)
}

/// Register a subscriber for `kinds` on the process-wide hub. The
/// receiver sees every watched family; filter on [`kind_of`].
pub fn subscribe(kinds: &[EventKind]) -> (Interest, broadcast::Receiver<pb::StateEvent>) {
    HUB.register(kinds)
}

/// Register the calling protocol task as a snapshot source.
pub fn snapshot_source() -> SnapshotSource {
    HUB.add_source()
}

/// Ask every snapshot source for the current state of `kinds`. The
/// returned channel carries the answers and closes when all sources
/// have answered. Subscribe first, so no change falls between.
pub fn request_snapshot(kinds: &[EventKind]) -> mpsc::UnboundedReceiver<pb::StateEvent> {
    HUB.request_snapshot(kinds)
}

/// Publish one state change. `vrf` is empty for the default VRF.
pub fn publish(vrf_id: u32, vrf: String, event: Event) {
    HUB.publish(vrf_id, vrf, event);
}

/// The family an event belongs to.
pub fn kind_of(event: &Event) -> EventKind {
    match event {
        Event::BgpPeer(_) => EventKind::BgpPeer,
        Event::BfdSession(_) => EventKind::BfdSession,
        Event::OspfNeighbor(_) => EventKind::OspfNeighbor,
        Event::Interface(_) => EventKind::Interface,
        Event::SpfRun(_) => EventKind::SpfRun,
        Event::IsisAdjacency(_) => EventKind::IsisAdjacency,
    }
}

/// Publish an applied SPF run. Shared by IS-IS and OSPF; `level` is
/// 0 and `area` empty where they do not apply.
pub fn publish_spf_run(
    ctx: &crate::context::ProtoContext,
    protocol: &str,
    level: u32,
    area: String,
    duration: std::time::Duration,
    nodes_reached: usize,
) {
    if !wants(EventKind::SpfRun) {
        return;
    }
    publish(
        ctx.vrf_id(),
        ctx.vrf_ifname().unwrap_or_default().to_string(),
        Event::SpfRun(pb::SpfRunEvent {
            protocol: protocol.to_string(),
            level,
            area,
            duration_us: duration.as_micros() as u64,
            nodes_reached: nodes_reached as u32,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A private hub per test, so tests don't share the global one.
    fn hub() -> &'static EventHub {
        Box::leak(Box::new(EventHub::new()))
    }

    fn bfd_event() -> Event {
        Event::BfdSession(pb::BfdSessionEvent::default())
    }

    #[test]
    fn interest_follows_subscriber_lifetime() {
        let hub = hub();
        assert!(!hub.wants(EventKind::BfdSession));
        let (interest, _rx) = hub.register(&[EventKind::BfdSession]);
        assert!(hub.wants(EventKind::BfdSession));
        assert!(!hub.wants(EventKind::BgpPeer));
        drop(interest);
        assert!(!hub.wants(EventKind::BfdSession));
    }

    #[test]
    fn seq_is_strictly_increasing() {
        let hub = hub();
        let (_interest, mut rx) = hub.register(&[EventKind::BfdSession]);
        hub.publish(0, String::new(), bfd_event());
        hub.publish(7, "blue".into(), bfd_event());
        let first = rx.try_recv().expect("first event");
        let second = rx.try_recv().expect("second event");
        assert_eq!((first.seq, second.seq), (1, 2));
        assert_eq!((second.vrf_id, second.vrf.as_str()), (7, "blue"));
        assert_eq!(
            kind_of(second.event.as_ref().unwrap()),
            EventKind::BfdSession
        );
    }

    #[test]
    fn unobserved_events_are_not_replayed() {
        let hub = hub();
        hub.publish(0, String::new(), bfd_event());
        let (_interest, mut rx) = hub.register(&[EventKind::BfdSession]);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn snapshot_reaches_every_source_and_closes_when_answered() {
        let hub = hub();
        let mut a = hub.add_source();
        let mut b = hub.add_source();
        let mut rx = hub.request_snapshot(&[EventKind::BfdSession]);
        let ra = a.recv().await.expect("request at a");
        let rb = b.recv().await.expect("request at b");
        assert!(ra.wants(EventKind::BfdSession) && !ra.wants(EventKind::BgpPeer));
        ra.publish(0, String::new(), bfd_event());
        drop(ra);
        rb.publish(0, String::new(), bfd_event());
        drop(rb);
        let first = rx.recv().await.expect("a's answer");
        let second = rx.recv().await.expect("b's answer");
        assert!(first.snapshot && second.snapshot);
        assert!(first.seq < second.seq);
        assert!(rx.recv().await.is_none(), "closed once both answered");
    }

    #[tokio::test]
    async fn snapshot_with_no_sources_closes_at_once() {
        let hub = hub();
        let mut rx = hub.request_snapshot(&[EventKind::BfdSession]);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_dropped_source_does_not_hold_the_snapshot_open() {
        let hub = hub();
        let source = hub.add_source();
        // Request queued, then the task ends without answering.
        let mut rx = hub.request_snapshot(&[EventKind::BfdSession]);
        drop(source);
        assert!(rx.recv().await.is_none());
        assert!(hub.sources.lock().unwrap().is_empty());
    }

    #[test]
    fn snapshot_and_live_events_share_one_sequence() {
        let hub = hub();
        let (_interest, mut live) = hub.register(&[EventKind::BfdSession]);
        let source = hub.add_source();
        let mut snap = hub.request_snapshot(&[EventKind::BfdSession]);
        let mut source = source;
        let req = source.rx.try_recv().expect("request");
        hub.publish(0, String::new(), bfd_event());
        req.publish(0, String::new(), bfd_event());
        hub.publish(0, String::new(), bfd_event());
        drop(req);
        let seqs: Vec<u64> = [
            live.try_recv().unwrap().seq,
            snap.try_recv().unwrap().seq,
            live.try_recv().unwrap().seq,
        ]
        .into();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn lagging_subscriber_is_told() {
        let hub = hub();
        let (_interest, mut rx) = hub.register(&[EventKind::BfdSession]);
        for _ in 0..CHANNEL_CAPACITY + 1 {
            hub.publish(0, String::new(), bfd_event());
        }
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(1))
        ));
    }
}
