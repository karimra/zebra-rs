//! `zebra.state.v1 StateService` — the gRPC face of the event hub.
//!
//! Each `Subscribe` registers interest in its families, then a
//! forwarder task copies matching hub events into the response stream
//! until the client goes away or falls behind.

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};

use super::{ALL_KINDS, EventKind, kind_of, pb, subscribe};

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

#[tonic::async_trait]
impl pb::state_service_server::StateService for StateSubscribeService {
    type SubscribeStream = ReceiverStream<Result<pb::StateEvent, Status>>;

    async fn subscribe(
        &self,
        request: tonic::Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let kinds = parse_kinds(&request.into_inner().kinds)?;
        let (interest, mut bus) = subscribe(&kinds);

        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            // Held for the life of the stream; dropping it on exit
            // withdraws this subscriber's interest.
            let _interest = interest;
            loop {
                tokio::select! {
                    event = bus.recv() => match event {
                        Ok(event) => {
                            // The hub carries every watched family;
                            // skip the ones another subscriber wanted.
                            let wanted = event
                                .event
                                .as_ref()
                                .is_some_and(|e| kinds.contains(&kind_of(e)));
                            if wanted && tx.send(Ok(event)).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(missed)) => {
                            let _ = tx
                                .send(Err(Status::resource_exhausted(format!(
                                    "subscriber fell behind, {missed} events dropped; resubscribe"
                                ))))
                                .await;
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = tx.closed() => break,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
