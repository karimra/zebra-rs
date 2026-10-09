@serial
@state_events
Feature: Operational state-change stream (zebra.state.v1)
  As a network operator
  I want protocol and interface state changes pushed to me as they happen
  So that automation can react without polling show commands.

  `vtyctl events --json` subscribes to every event family on z1 before
  any protocol is configured, so the stream must carry the whole
  session bring-up, and then the teardown when z1's link goes down.

  Test Topology:
  ```
  ┌─────────────────────────────────────────┐
  │                   br0                   │
  └─────────────┬───────────────┬───────────┘
                │               │
           ┌────┴────┐     ┌────┴────┐
           │   z1    │     │   z2    │
           │ AS65001 │     │ AS65002 │
           │192.168. │     │192.168. │
           │  0.1/24 │     │  0.2/24 │
           └─────────┘     └─────────┘
  ```

  Config files:
  - z1.yaml: eBGP to 192.168.0.2, OSPF area 0 on vz1ns
  - z2.yaml: eBGP to 192.168.0.1, OSPF area 0 on vz2ns

  Scenario: Session bring-up is streamed
    Given a clean test environment
    When I create bridge "br0"
    And I create namespace "z1" with IP "192.168.0.1/24" on bridge "br0"
    And I create namespace "z2" with IP "192.168.0.2/24" on bridge "br0"
    And I start zebra-rs in namespace "z1"
    And I start zebra-rs in namespace "z2"
    And I start streaming state events in namespace "z1"
    And I apply config "z1.yaml" to namespace "z1"
    And I apply config "z2.yaml" to namespace "z2"
    Then state events in "z1" should eventually include "type=bgp-peer peer-address=192.168.0.2 old-state=OPEN_CONFIRM new-state=ESTABLISHED"
    And state events in "z1" should eventually include "type=ospf-neighbor router-id=192.168.0.2 interface=vz1ns new-state=FULL"
    And state events in "z1" should eventually include "type=spf-run protocol=ospf area=0.0.0.0"
    And state event sequence numbers in "z1" should be strictly increasing

  Scenario: Link down is streamed with its consequences
    Given the test topology exists
    When I bring link down in namespace "z1"
    Then state events in "z1" should eventually include "type=interface name=vz1ns change=DOWN"
    And state events in "z1" should eventually include "type=ospf-neighbor router-id=192.168.0.2 new-state=DOWN reason=InterfaceDown"
    And state events in "z1" should eventually include "type=bgp-peer peer-address=192.168.0.2 old-state=ESTABLISHED"
    And state event sequence numbers in "z1" should be strictly increasing

  Scenario: Teardown topology
    # Separate scenario so cleanup still runs when a step above fails
    # (a failed step skips the rest of its own scenario only). Stopping
    # the daemon also ends the `vtyctl events` stream.
    When I stop zebra-rs in namespace "z1"
    And I stop zebra-rs in namespace "z2"
    And I delete namespace "z1"
    And I delete namespace "z2"
    And I delete bridge "br0"
    Then the test environment should be clean
