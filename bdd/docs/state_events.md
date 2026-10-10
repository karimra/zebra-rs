# Operational state-change stream (zebra.state.v1)

## Overview

As a network operator
I want protocol and interface state changes pushed to me as they happen
So that automation can react without polling show commands.

`vtyctl events --json` subscribes to every event family on z1 before
any protocol is configured, so the stream must carry the whole
session bring-up, and then the teardown when z1's link goes down.
A second subscriber joins once everything is up and asks for a
snapshot, so it must be told the current state without any change
happening.

## Test Topology

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

## Config Files

- z1.yaml: eBGP to 192.168.0.2, OSPF area 0 on vz1ns
- z2.yaml: eBGP to 192.168.0.1, OSPF area 0 on vz2ns

## Test Scenarios

| Scenario | Result |
|----------|--------|
| Session bring-up is streamed | |
| A late subscriber gets the current state as a snapshot | |
| Link down is streamed with its consequences | |
| Teardown topology | |
