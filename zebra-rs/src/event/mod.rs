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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

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
const ALL_KINDS: [EventKind; 2] = [EventKind::BgpPeer, EventKind::BfdSession];

/// One slot per `EventKind` discriminant (slot 0, UNSPECIFIED, unused).
const KIND_SLOTS: usize = ALL_KINDS.len() + 1;

static HUB: LazyLock<EventHub> = LazyLock::new(EventHub::new);

pub struct EventHub {
    /// Last sequence number handed out, together with the sender.
    /// Allocating the number and sending under one lock makes `seq`
    /// order equal delivery order across publishing tasks. The lock is
    /// only taken while some family is watched.
    tx: Mutex<(u64, broadcast::Sender<pb::StateEvent>)>,
    /// Subscribers selecting each family, indexed by discriminant.
    interest: [AtomicUsize; KIND_SLOTS],
}

impl EventHub {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            tx: Mutex::new((0, tx)),
            interest: Default::default(),
        }
    }

    fn wants(&self, kind: EventKind) -> bool {
        self.interest[kind as usize].load(Ordering::Relaxed) > 0
    }

    fn publish(&self, vrf_id: u32, vrf: String, event: Event) {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut guard = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let (seq, tx) = &mut *guard;
        *seq += 1;
        // Err only means nobody is subscribed right now — the last
        // subscriber left between the caller's `wants` and here.
        let _ = tx.send(pb::StateEvent {
            seq: *seq,
            timestamp_ns,
            vrf_id,
            vrf,
            event: Some(event),
        });
    }

    /// Register a subscriber for `kinds`. Interest is counted from
    /// here until the returned [`Interest`] is dropped.
    fn register(
        &'static self,
        kinds: &[EventKind],
    ) -> (Interest, broadcast::Receiver<pb::StateEvent>) {
        let rx = self
            .tx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .subscribe();
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

/// Publish one state change. `vrf` is empty for the default VRF.
pub fn publish(vrf_id: u32, vrf: String, event: Event) {
    HUB.publish(vrf_id, vrf, event);
}

/// The family an event belongs to.
pub fn kind_of(event: &Event) -> EventKind {
    match event {
        Event::BgpPeer(_) => EventKind::BgpPeer,
        Event::BfdSession(_) => EventKind::BfdSession,
    }
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
