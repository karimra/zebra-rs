use isis_packet::*;
use num_enum::IntoPrimitive;
use strum_macros::{Display, EnumString};

use crate::context::Timer;
use crate::isis::rib::spf_schedule;

use super::link::LinkTop;
use super::{IfsmEvent, Level, Message};

use super::neigh::Neighbor;

// Neighbor state. The value corresponds to P2P Hello three way handshke TLV's
// state value.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Display, EnumString, IntoPrimitive)]
pub enum NfsmState {
    #[strum(serialize = "Up")]
    Up = 0,
    #[strum(serialize = "Init")]
    Init = 1,
    #[strum(serialize = "Down")]
    Down = 2,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Display, EnumString)]
pub enum NfsmEvent {
    #[strum(serialize = "HoldTimerExpire")]
    HoldTimerExpire,
}

pub fn nfsm_hold_timer(nbr: &Neighbor, level: Level) -> Timer {
    let tx = nbr.tx.clone();
    let sys_id = nbr.sys_id;
    let ifindex = nbr.ifindex;
    Timer::once(nbr.hold_time as u64, move || {
        let tx = tx.clone();
        let sys_id = sys_id;
        async move {
            tx.send(Message::Nfsm(
                NfsmEvent::HoldTimerExpire,
                level,
                ifindex,
                sys_id,
            ))
            .unwrap();
        }
    })
}

/// Publish a `zebra.state.v1` adjacency transition. Every site that
/// sets `nbr.state` or drops a neighbor must call this, or subscribers
/// miss the change. No-op when nothing changed or nobody watches.
#[allow(clippy::too_many_arguments)]
pub fn publish_adj_event(
    ctx: &crate::context::ProtoContext,
    ifname: &str,
    hostnames: &super::Levels<super::hostname::Hostname>,
    level: Level,
    nbr: &Neighbor,
    old: NfsmState,
    new: NfsmState,
    reason: &str,
) {
    use crate::event::{self, EventKind, pb};

    if old == new || !event::wants(EventKind::IsisAdjacency) {
        return;
    }
    fn state(s: NfsmState) -> pb::IsisAdjacencyState {
        match s {
            NfsmState::Down => pb::IsisAdjacencyState::Down,
            NfsmState::Init => pb::IsisAdjacencyState::Init,
            NfsmState::Up => pb::IsisAdjacencyState::Up,
        }
    }
    let hostname = hostnames
        .get(&level)
        .get(&nbr.sys_id)
        .map(|(name, _)| name.clone())
        .unwrap_or_default();
    event::publish(
        ctx.vrf_id(),
        ctx.vrf_ifname().unwrap_or_default().to_string(),
        event::Event::IsisAdjacency(pb::IsisAdjacencyEvent {
            system_id: nbr.sys_id.to_string(),
            hostname,
            interface: ifname.to_string(),
            level: level.digit() as u32,
            old_state: state(old) as i32,
            new_state: state(new) as i32,
            reason: reason.to_string(),
        }),
    );
}

pub fn nbr_hold_timer_expire(
    link: &mut LinkTop,
    level: Level,
    sys_id: IsisSysId,
    release_bfd: bool,
) {
    use IfsmEvent::*;

    // RFC 5882 §3.2: this adjacency is being torn down for real, so drop any
    // BFD hold-down pin for it (a no-op when none is set).
    link.state.bfd_holddown.get_mut(&level).remove(&sys_id);
    link.state
        .bfd_holddown_nbr
        .retain(|_, (l, s)| *l != level || *s != sys_id);

    let is_p2p = link.is_p2p();

    // When the link is P2P, cancel CSNP timer. For LAN interface, CSNP timer
    // will be handled by DIS election.
    if is_p2p {
        *link.timer.csnp.get_mut(&level) = None;
        *link.state.adj.get_mut(&level) = None;
        link.lsdb.get_mut(&level).adj_clear(link.ifindex);
    }

    // Find neighbor.
    let Some(nbr) = link.state.nbrs.get_mut(&level).get_mut(&sys_id) else {
        return;
    };

    let was_up = nbr.state == NfsmState::Up;
    let ifindex = nbr.ifindex;
    publish_adj_event(
        link.ctx,
        &link.state.name,
        link.hostname,
        level,
        nbr,
        nbr.state,
        NfsmState::Down,
        if release_bfd {
            "HoldTimerExpired"
        } else {
            "BfdDown"
        },
    );
    // Snapshot the peer's IPv4 and IPv6 link-local so we can send a BFD
    // Unsubscribe after dropping the nbr borrow. nbr's addr maps are about to
    // be wiped along with the entire entry below. Must be the same pick the
    // subscribe path used (packet::bfd_session_key) or the Unsubscribe key
    // won't match the live session.
    let bfd_peer_v4 = super::link::nbr_v4_pick(&link.state.v4addr, &nbr.addr4);
    let bfd_peer_v6ll = super::link::nbr_v6ll_pick(&nbr.addr6l);

    // Originate Hello and LSP.
    if is_p2p {
        nbr.event(Message::Ifsm(HelloOriginate, ifindex, Some(level)));
        nbr.event(Message::LspOriginate(level, None));
    } else {
        // DIS election. Run before we drop the entry so the snapshot
        // it operates on still has this neighbor.
        nbr.event(Message::Ifsm(HelloOriginate, ifindex, Some(level)));
        if was_up {
            nbr.event(Message::Ifsm(DisSelection, ifindex, Some(level)));
        }
    }

    // Release labels.
    for value in nbr.addr4.values_mut() {
        if let Some(label) = value.label {
            if let Some(local_pool) = link.local_pool {
                local_pool.release(label);
            }
            value.label = None;
        }
    }

    // Release the End.X SID — function bits go back to the ELIB pool
    // and the registry drops the row before the show table runs again.
    nbr.release_endx_sid(link.elib, link.rib_client);

    // Drop the neighbor entry. Keeping it around with NfsmState::Down
    // (the previous behaviour) carried stale `addr4` (whose labels we
    // just released and zeroed), the consumed `endx_sid` slot, and a
    // newly-armed `hold_timer` from any in-flight hello that still
    // looked up this entry. When the peer's interface bounces and
    // hellos resume, `nbr_hello_interpret` would find the lingering
    // record, retain the existing addr entries with `label=None`
    // forever (no LSP adj-SID re-emission), and re-enter NFSM from
    // a half-populated state. Drop the row so the next hello starts
    // from scratch.
    link.state.nbrs.get_mut(&level).remove(&sys_id);
    if was_up {
        let counter = link.state.nbrs_up.get_mut(&level);
        *counter = counter.saturating_sub(1);
    }

    // RFC 5882 §5: if the timed-out adjacency was Up and had BFD attached,
    // release the BFD session — but only when `release_bfd` is set. The
    // BFD-down hold-down path passes `false` so the session keeps probing and
    // can detect the peer returning. Idempotent with the packet.rs regression
    // path that may already have sent the Unsubscribe.
    if release_bfd
        && was_up
        && link.config.bfd.resolve(&link.up_config.bfd).enable
        && let Some(key) = super::packet::bfd_session_key(link, bfd_peer_v4, bfd_peer_v6ll)
    {
        let _ = link.tx.send(Message::BfdUnsubscribe(key));
    }

    // The Up adjacency is gone — let the STAMP reconcile see the
    // removed neighbor and release the measurement session (diff-gated
    // no-op when none exists).
    if was_up {
        let _ = link.tx.send(Message::StampReconcile(link.ifindex));
    }

    spf_schedule(link, level);
}

#[cfg(test)]
mod state_event_tests {
    use super::super::Levels;
    use super::super::hostname::Hostname;
    use super::super::link::NetworkType;
    use super::*;
    use crate::context::ProtoContext;
    use crate::event::{self, EventKind, pb};

    fn sys(b: u8) -> IsisSysId {
        IsisSysId {
            id: [0, 0, 0, 0, 0xee, b],
        }
    }

    /// The hub is process-wide and tests run in parallel, so pick this
    /// test's event out by its (unique) system id.
    fn next_for(
        rx: &mut tokio::sync::broadcast::Receiver<pb::StateEvent>,
        sys_id: &str,
    ) -> Option<pb::IsisAdjacencyEvent> {
        while let Ok(ev) = rx.try_recv() {
            if let Some(event::Event::IsisAdjacency(a)) = ev.event
                && a.system_id == sys_id
            {
                return Some(a);
            }
        }
        None
    }

    #[tokio::test]
    async fn adjacency_down_carries_hostname_level_and_reason() {
        let (_interest, mut rx) = event::subscribe(&[EventKind::IsisAdjacency]);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let nbr = Neighbor::new(tx, 2, NetworkType::P2p, sys(1), None);
        let mut hostnames: Levels<Hostname> = Levels::default();
        hostnames.l2.insert(sys(1), "r2".into());
        publish_adj_event(
            &ProtoContext::default_table_no_rib(),
            "eth0",
            &hostnames,
            Level::L2,
            &nbr,
            NfsmState::Up,
            NfsmState::Down,
            "HoldTimerExpired",
        );
        let a = next_for(&mut rx, &sys(1).to_string()).expect("Up -> Down event");
        assert_eq!((a.hostname.as_str(), a.interface.as_str()), ("r2", "eth0"));
        assert_eq!(a.level, 2);
        assert_eq!(a.old_state, pb::IsisAdjacencyState::Up as i32);
        assert_eq!(a.new_state, pb::IsisAdjacencyState::Down as i32);
        assert_eq!(a.reason, "HoldTimerExpired");
    }

    #[tokio::test]
    async fn unchanged_state_is_not_published() {
        let (_interest, mut rx) = event::subscribe(&[EventKind::IsisAdjacency]);
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let nbr = Neighbor::new(tx, 2, NetworkType::P2p, sys(2), None);
        publish_adj_event(
            &ProtoContext::default_table_no_rib(),
            "eth0",
            &Levels::default(),
            Level::L1,
            &nbr,
            NfsmState::Init,
            NfsmState::Init,
            "",
        );
        assert!(next_for(&mut rx, &sys(2).to_string()).is_none());
    }
}
