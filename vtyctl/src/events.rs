use anyhow::{Result, bail};
use std::io::Write;
use std::process::exit;
use tonic::Request;

use pb::state_event::Event;
use pb::state_service_client::StateServiceClient;
use pb::{
    BfdState, BgpState, EventKind, InterfaceChange, IsisAdjacencyState, OspfNeighborState,
    StateEvent, SubscribeRequest,
};

pub mod pb {
    tonic::include_proto!("zebra.state.v1");
}

/// Map a command-line family name to its `EventKind`.
fn parse_kind(name: &str) -> Result<EventKind> {
    match name {
        "bgp" | "bgp-peer" => Ok(EventKind::BgpPeer),
        "bfd" | "bfd-session" => Ok(EventKind::BfdSession),
        "ospf" | "ospf-neighbor" => Ok(EventKind::OspfNeighbor),
        "interface" | "if" => Ok(EventKind::Interface),
        "spf" | "spf-run" => Ok(EventKind::SpfRun),
        "isis" | "isis-adjacency" => Ok(EventKind::IsisAdjacency),
        _ => {
            bail!("unknown event family '{name}' (expected bgp, bfd, ospf, isis, interface or spf)")
        }
    }
}

/// Display form of a state: its proto name without the type prefix
/// (`BGP_STATE_OPEN_SENT` -> `OPEN_SENT`).
fn state_name(full: &str, prefix: &str) -> String {
    full.strip_prefix(prefix).unwrap_or(full).to_string()
}

fn bgp_state(v: i32) -> String {
    BgpState::try_from(v)
        .map(|s| state_name(s.as_str_name(), "BGP_STATE_"))
        .unwrap_or_else(|_| v.to_string())
}

fn bfd_state(v: i32) -> String {
    BfdState::try_from(v)
        .map(|s| state_name(s.as_str_name(), "BFD_STATE_"))
        .unwrap_or_else(|_| v.to_string())
}

fn ospf_state(v: i32) -> String {
    OspfNeighborState::try_from(v)
        .map(|s| state_name(s.as_str_name(), "OSPF_NEIGHBOR_STATE_"))
        .unwrap_or_else(|_| v.to_string())
}

fn isis_state(v: i32) -> String {
    IsisAdjacencyState::try_from(v)
        .map(|s| state_name(s.as_str_name(), "ISIS_ADJACENCY_STATE_"))
        .unwrap_or_else(|_| v.to_string())
}

fn interface_change(v: i32) -> String {
    InterfaceChange::try_from(v)
        .map(|c| state_name(c.as_str_name(), "INTERFACE_CHANGE_"))
        .unwrap_or_else(|_| v.to_string())
}

/// `OLD -> NEW` for a change; `is NEW` for a snapshot object.
fn transition(snapshot: bool, old: String, new: String) -> String {
    if snapshot {
        format!("is {new}")
    } else {
        format!("{old} -> {new}")
    }
}

/// ` reason: …` suffix, or nothing when there is no reason.
fn reason_suffix(reason: &str) -> String {
    if reason.is_empty() {
        String::new()
    } else {
        format!(" reason: {reason}")
    }
}

/// One event as a single human-readable line.
fn format_event(ev: &StateEvent) -> String {
    if ev.snapshot_complete {
        return "--- snapshot complete".to_string();
    }
    let secs = ev.timestamp_ns / 1_000_000_000;
    let millis = (ev.timestamp_ns % 1_000_000_000) / 1_000_000;
    let vrf = if ev.vrf.is_empty() {
        String::new()
    } else {
        format!(" vrf {}", ev.vrf)
    };
    let body = match &ev.event {
        Some(Event::BgpPeer(p)) => {
            let ifname = if p.interface.is_empty() {
                String::new()
            } else {
                format!(" ({})", p.interface)
            };
            let reason = reason_suffix(&p.down_reason);
            format!(
                "bgp neighbor {}{ifname} AS{} {}{reason}",
                p.peer_address,
                p.remote_as,
                transition(ev.snapshot, bgp_state(p.old_state), bgp_state(p.new_state)),
            )
        }
        Some(Event::BfdSession(b)) => format!(
            "bfd {} -> {}{} {} diag: {}",
            b.local_address,
            b.remote_address,
            if b.multihop { " multihop" } else { "" },
            transition(ev.snapshot, bfd_state(b.old_state), bfd_state(b.new_state)),
            b.diagnostic,
        ),
        Some(Event::OspfNeighbor(o)) => format!(
            "{} neighbor {} ({}) on {} {}{}",
            if o.version == 3 { "ospfv3" } else { "ospf" },
            o.router_id,
            o.neighbor_address,
            o.interface,
            transition(
                ev.snapshot,
                ospf_state(o.old_state),
                ospf_state(o.new_state)
            ),
            reason_suffix(&o.reason),
        ),
        Some(Event::Interface(i)) => {
            let address = if i.address.is_empty() {
                String::new()
            } else {
                format!(" {}", i.address)
            };
            format!(
                "interface {} (ifindex {}) {}{address}",
                i.name,
                i.ifindex,
                interface_change(i.change),
            )
        }
        Some(Event::SpfRun(r)) => {
            let scope = if r.protocol == "isis" {
                format!("level-{}", r.level)
            } else {
                format!("area {}", r.area)
            };
            format!(
                "{} spf {scope} {} nodes in {} us",
                r.protocol, r.nodes_reached, r.duration_us
            )
        }
        Some(Event::IsisAdjacency(a)) => {
            let host = if a.hostname.is_empty() {
                String::new()
            } else {
                format!(" ({})", a.hostname)
            };
            format!(
                "isis adjacency {}{host} on {} level-{} {}{}",
                a.system_id,
                a.interface,
                a.level,
                transition(
                    ev.snapshot,
                    isis_state(a.old_state),
                    isis_state(a.new_state)
                ),
                reason_suffix(&a.reason),
            )
        }
        None => "unknown event".to_string(),
    };
    let snapshot = if ev.snapshot { " [snapshot]" } else { "" };
    format!("{secs}.{millis:03} #{}{snapshot}{vrf} {body}", ev.seq)
}

pub async fn events(host: &str, json: bool, snapshot: bool, families: &[String]) -> Result<()> {
    let kinds = families
        .iter()
        .map(|f| parse_kind(f).map(|k| k as i32))
        .collect::<Result<Vec<_>>>()?;

    let uri = crate::endpoint::host_uri(host);
    let channel = match crate::endpoint::connect(&uri).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Can't connect to {uri}: {e}");
            exit(3);
        }
    };
    let mut client = StateServiceClient::new(channel);
    let mut stream = client
        .subscribe(Request::new(SubscribeRequest { kinds, snapshot }))
        .await?
        .into_inner();

    while let Some(ev) = stream.message().await? {
        if json {
            println!("{}", serde_json::to_string(&event_json(&ev))?);
        } else {
            println!("{}", format_event(&ev));
        }
        std::io::stdout().flush()?;
    }
    Ok(())
}

/// One event as a JSON object (prost types carry no serde derive).
fn event_json(ev: &StateEvent) -> serde_json::Value {
    use serde_json::json;
    if ev.snapshot_complete {
        return json!({ "snapshot-complete": true });
    }
    let event = match &ev.event {
        Some(Event::BgpPeer(p)) => json!({
            "type": "bgp-peer",
            "peer-address": p.peer_address,
            "interface": p.interface,
            "local-as": p.local_as,
            "remote-as": p.remote_as,
            "old-state": bgp_state(p.old_state),
            "new-state": bgp_state(p.new_state),
            "down-reason": p.down_reason,
        }),
        Some(Event::BfdSession(b)) => json!({
            "type": "bfd-session",
            "local-address": b.local_address,
            "remote-address": b.remote_address,
            "ifindex": b.ifindex,
            "multihop": b.multihop,
            "old-state": bfd_state(b.old_state),
            "new-state": bfd_state(b.new_state),
            "diagnostic": b.diagnostic,
        }),
        Some(Event::OspfNeighbor(o)) => json!({
            "type": "ospf-neighbor",
            "version": o.version,
            "router-id": o.router_id,
            "neighbor-address": o.neighbor_address,
            "interface": o.interface,
            "old-state": ospf_state(o.old_state),
            "new-state": ospf_state(o.new_state),
            "reason": o.reason,
        }),
        Some(Event::Interface(i)) => json!({
            "type": "interface",
            "name": i.name,
            "ifindex": i.ifindex,
            "change": interface_change(i.change),
            "address": i.address,
        }),
        Some(Event::SpfRun(r)) => json!({
            "type": "spf-run",
            "protocol": r.protocol,
            "level": r.level,
            "area": r.area,
            "duration-us": r.duration_us,
            "nodes-reached": r.nodes_reached,
        }),
        Some(Event::IsisAdjacency(a)) => json!({
            "type": "isis-adjacency",
            "system-id": a.system_id,
            "hostname": a.hostname,
            "interface": a.interface,
            "level": a.level,
            "old-state": isis_state(a.old_state),
            "new-state": isis_state(a.new_state),
            "reason": a.reason,
        }),
        None => json!({ "type": "unknown" }),
    };
    json!({
        "seq": ev.seq,
        "timestamp-ns": ev.timestamp_ns,
        "vrf-id": ev.vrf_id,
        "vrf": ev.vrf,
        "snapshot": ev.snapshot,
        "event": event,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::BgpPeerEvent;

    #[test]
    fn bgp_event_renders_on_one_line() {
        let ev = StateEvent {
            seq: 7,
            timestamp_ns: 1_700_000_000_123_000_000,
            vrf_id: 10,
            vrf: "blue".into(),
            snapshot: false,
            snapshot_complete: false,
            event: Some(Event::BgpPeer(BgpPeerEvent {
                peer_address: "10.0.0.2".into(),
                interface: String::new(),
                local_as: 65001,
                remote_as: 65002,
                old_state: BgpState::Established as i32,
                new_state: BgpState::Idle as i32,
                down_reason: "Hold timer expired".into(),
            })),
        };
        assert_eq!(
            format_event(&ev),
            "1700000000.123 #7 vrf blue bgp neighbor 10.0.0.2 AS65002 ESTABLISHED -> IDLE reason: Hold timer expired"
        );
    }

    #[test]
    fn snapshot_objects_and_marker_render() {
        let ev = StateEvent {
            seq: 3,
            snapshot: true,
            event: Some(Event::BgpPeer(BgpPeerEvent {
                peer_address: "10.0.0.2".into(),
                remote_as: 65002,
                new_state: BgpState::Established as i32,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            format_event(&ev),
            "0.000 #3 [snapshot] bgp neighbor 10.0.0.2 AS65002 is ESTABLISHED"
        );
        let done = StateEvent {
            snapshot_complete: true,
            ..Default::default()
        };
        assert_eq!(format_event(&done), "--- snapshot complete");
        assert_eq!(event_json(&done)["snapshot-complete"], true);
    }

    #[test]
    fn family_names_parse() {
        assert_eq!(parse_kind("bgp").unwrap(), EventKind::BgpPeer);
        assert_eq!(parse_kind("bfd-session").unwrap(), EventKind::BfdSession);
        assert_eq!(parse_kind("ospf").unwrap(), EventKind::OspfNeighbor);
        assert_eq!(parse_kind("interface").unwrap(), EventKind::Interface);
        assert_eq!(parse_kind("spf").unwrap(), EventKind::SpfRun);
        assert_eq!(parse_kind("isis").unwrap(), EventKind::IsisAdjacency);
        assert!(parse_kind("rip").is_err());
    }
}
