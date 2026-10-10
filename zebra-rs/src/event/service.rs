//! `zebra.state.v1 StateService` — the gRPC face of the event hub.
//!
//! Each `Subscribe` registers interest in its families, then a
//! forwarder task copies matching hub events into the response stream
//! until the client goes away or falls behind.

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use super::{ALL_KINDS, EventKind, kind_of, pb, request_snapshot, subscribe};

pub use pb::state_service_server::StateServiceServer;

#[derive(Debug, Default)]
pub struct StateSubscribeService;

/// Validate the requested families; empty selects all of them.
fn parse_kinds(raw: &[i32]) -> Result<Vec<EventKind>, Status> {
    if raw.is_empty() {
        return Ok(ALL_KINDS.to_vec());
    }
    let mut kinds = Vec::with_capacity(raw.len());
    for &k in raw {
        match EventKind::try_from(k) {
            Ok(EventKind::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(format!("unknown event kind {k}")));
            }
            Ok(kind) => {
                if !kinds.contains(&kind) {
                    kinds.push(kind);
                }
            }
        }
    }
    Ok(kinds)
}

/// What the forwarder hands the client next.
#[derive(Debug, PartialEq)]
enum Next {
    Event(pb::StateEvent),
    /// Every snapshot source has answered.
    SnapshotComplete,
    /// The live channel overran; this many events were lost.
    Lagged(u64),
    Closed,
}

/// Merges live changes with an optional in-flight snapshot into one
/// stream in `seq` order.
///
/// Both channels are filled under the hub's sequence lock in `seq`
/// order, so once an event with seq N has been received from one
/// channel, every event below N is already queued in the other. The
/// merge relies on that: it only emits from one side after polling the
/// other side afresh, and when both have an event it emits the lower.
struct Merge {
    kinds: Vec<EventKind>,
    live: broadcast::Receiver<pb::StateEvent>,
    snap: Option<mpsc::UnboundedReceiver<pb::StateEvent>>,
    live_next: Option<pb::StateEvent>,
    snap_next: Option<pb::StateEvent>,
}

impl Merge {
    fn new(
        kinds: Vec<EventKind>,
        live: broadcast::Receiver<pb::StateEvent>,
        snap: Option<mpsc::UnboundedReceiver<pb::StateEvent>>,
    ) -> Self {
        Self {
            kinds,
            live,
            snap,
            live_next: None,
            snap_next: None,
        }
    }

    fn wanted(&self, ev: &pb::StateEvent) -> bool {
        ev.event
            .as_ref()
            .is_some_and(|e| self.kinds.contains(&kind_of(e)))
    }

    /// Take a queued live event into `live_next` if there is one.
    /// The hub carries every watched family, so skip the ones another
    /// subscriber wanted.
    fn poll_live(&mut self) -> Option<Next> {
        while self.live_next.is_none() {
            match self.live.try_recv() {
                Ok(ev) if self.wanted(&ev) => self.live_next = Some(ev),
                Ok(_) => continue,
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(n)) => return Some(Next::Lagged(n)),
                Err(broadcast::error::TryRecvError::Closed) => return Some(Next::Closed),
            }
        }
        None
    }

    /// Take a queued snapshot event into `snap_next` if there is one;
    /// report completion once the snapshot channel is drained and
    /// closed.
    fn poll_snap(&mut self) -> Option<Next> {
        if self.snap_next.is_some() {
            return None;
        }
        let snap = self.snap.as_mut()?;
        match snap.try_recv() {
            Ok(ev) => self.snap_next = Some(ev),
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                self.snap = None;
                return Some(Next::SnapshotComplete);
            }
        }
        None
    }

    async fn next(&mut self) -> Next {
        loop {
            if let Some(n) = self.poll_live() {
                return n;
            }
            if let Some(n) = self.poll_snap() {
                return n;
            }
            // Holding one side only: poll the other again now. Anything
            // below the held event's seq is queued by now.
            if self.live_next.is_some() != self.snap_next.is_some() {
                let again = if self.live_next.is_some() {
                    self.poll_snap()
                } else {
                    self.poll_live()
                };
                if let Some(n) = again {
                    return n;
                }
            }
            match (&self.live_next, &self.snap_next) {
                (Some(l), Some(s)) => {
                    return Next::Event(if l.seq < s.seq {
                        self.live_next.take().unwrap()
                    } else {
                        self.snap_next.take().unwrap()
                    });
                }
                (Some(_), None) => return Next::Event(self.live_next.take().unwrap()),
                (None, Some(_)) => return Next::Event(self.snap_next.take().unwrap()),
                (None, None) => {}
            }
            // Nothing queued anywhere: wait for either side, then loop
            // to re-run the ordering checks.
            match self.snap.as_mut() {
                Some(snap) => tokio::select! {
                    r = self.live.recv() => match r {
                        Ok(ev) if self.wanted(&ev) => self.live_next = Some(ev),
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(n)) => return Next::Lagged(n),
                        Err(broadcast::error::RecvError::Closed) => return Next::Closed,
                    },
                    r = snap.recv() => match r {
                        Some(ev) => self.snap_next = Some(ev),
                        None => {
                            self.snap = None;
                            return Next::SnapshotComplete;
                        }
                    },
                },
                None => match self.live.recv().await {
                    Ok(ev) if self.wanted(&ev) => self.live_next = Some(ev),
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(n)) => return Next::Lagged(n),
                    Err(broadcast::error::RecvError::Closed) => return Next::Closed,
                },
            }
        }
    }
}

#[tonic::async_trait]
impl pb::state_service_server::StateService for StateSubscribeService {
    type SubscribeStream = ReceiverStream<Result<pb::StateEvent, Status>>;

    async fn subscribe(
        &self,
        request: tonic::Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        let kinds = parse_kinds(&request.kinds)?;
        // Live first, then the snapshot: a change racing the snapshot
        // then reaches the client one way or the other, in seq order.
        let (interest, live) = subscribe(&kinds);
        let snap = request.snapshot.then(|| request_snapshot(&kinds));
        let mut merge = Merge::new(kinds, live, snap);

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            // Held for the life of the stream; dropping it on exit
            // withdraws this subscriber's interest.
            let _interest = interest;
            loop {
                let item = tokio::select! {
                    next = merge.next() => next,
                    _ = tx.closed() => break,
                };
                let out = match item {
                    Next::Event(ev) => Ok(ev),
                    Next::SnapshotComplete => Ok(pb::StateEvent {
                        snapshot_complete: true,
                        ..Default::default()
                    }),
                    Next::Lagged(missed) => {
                        let _ = tx
                            .send(Err(Status::resource_exhausted(format!(
                                "subscriber fell behind, {missed} events dropped; resubscribe"
                            ))))
                            .await;
                        break;
                    }
                    Next::Closed => break,
                };
                if tx.send(out).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(seq: u64, snapshot: bool) -> pb::StateEvent {
        pb::StateEvent {
            seq,
            snapshot,
            event: Some(super::super::Event::BfdSession(
                pb::BfdSessionEvent::default(),
            )),
            ..Default::default()
        }
    }

    fn seqs(items: &[Next]) -> Vec<(u64, bool)> {
        items
            .iter()
            .filter_map(|n| match n {
                Next::Event(e) => Some((e.seq, e.snapshot)),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn merge_orders_live_and_snapshot_by_seq() {
        let (ltx, lrx) = broadcast::channel(16);
        let (stx, srx) = mpsc::unbounded_channel();
        // Sent in seq order across both channels, as the hub does.
        ltx.send(ev(1, false)).unwrap();
        stx.send(ev(2, true)).unwrap();
        stx.send(ev(3, true)).unwrap();
        ltx.send(ev(4, false)).unwrap();
        stx.send(ev(5, true)).unwrap();
        drop(stx);
        let mut m = Merge::new(vec![EventKind::BfdSession], lrx, Some(srx));
        let mut got = Vec::new();
        for _ in 0..6 {
            got.push(m.next().await);
        }
        assert_eq!(
            seqs(&got),
            vec![(1, false), (2, true), (3, true), (4, false), (5, true)]
        );
        assert_eq!(got[5], Next::SnapshotComplete);
        // Afterwards it is a plain live stream.
        ltx.send(ev(6, false)).unwrap();
        assert_eq!(seqs(&[m.next().await]), vec![(6, false)]);
    }

    #[tokio::test]
    async fn merge_skips_unselected_families() {
        let (ltx, lrx) = broadcast::channel(16);
        let mut other = ev(1, false);
        other.event = Some(super::super::Event::BgpPeer(pb::BgpPeerEvent::default()));
        ltx.send(other).unwrap();
        ltx.send(ev(2, false)).unwrap();
        let mut m = Merge::new(vec![EventKind::BfdSession], lrx, None);
        assert_eq!(seqs(&[m.next().await]), vec![(2, false)]);
    }

    #[tokio::test]
    async fn merge_reports_lag() {
        let (ltx, lrx) = broadcast::channel(2);
        for seq in 1..=3 {
            ltx.send(ev(seq, false)).unwrap();
        }
        let mut m = Merge::new(vec![EventKind::BfdSession], lrx, None);
        assert_eq!(m.next().await, Next::Lagged(1));
    }

    #[test]
    fn empty_kinds_select_everything() {
        assert_eq!(parse_kinds(&[]).unwrap(), ALL_KINDS.to_vec());
    }

    #[test]
    fn kinds_are_deduplicated() {
        let bgp = EventKind::BgpPeer as i32;
        assert_eq!(parse_kinds(&[bgp, bgp]).unwrap(), vec![EventKind::BgpPeer]);
    }

    #[test]
    fn unknown_and_unspecified_kinds_are_rejected() {
        assert!(parse_kinds(&[0]).is_err());
        assert!(parse_kinds(&[99]).is_err());
    }
}
