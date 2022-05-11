# Cyphal network bridge & router PoC

This is a visual aid for https://forum.opencyphal.org/t/cyphal-udp-routing-over-multiple-networks/1657/7?u=pavel.kirienko

Currently we are using Cyphal/serial instead of Cyphal/UDP because the bridge relies on the transfer spoofing feature
and it is at this moment not yet implemented for `pycyphal.transport.udp.UDPTransport`.
It makes no difference though.

## Preparation

1. Initialize `vcan0` and `vcan1` like:
  ```shell
  modprobe can
  modprobe can_raw
  modprobe vcan
  ip link add dev vcan0 type vcan
  ip link set vcan0 mtu 72         # Enable CAN FD by configuring the MTU of 64+8
  ip link set up vcan0
  ```
2. `y co https://github.com/OpenCyphal/public_regulated_data_types/archive/refs/heads/master.zip`
3. Open a few terminals and source `env_vcan0.sh` in some of them and `env_vcan1.sh` in others.
   These are the separate CAN network segments.
   Run some publishers/subscribers like:
   - On `vcan0`: `y pub 1000:uavcan.primitive.scalar.integer64 '!$ n'`
   - On `vcan1`: `y sub 1000:uavcan.primitive.scalar.integer64`
4. Observe that there is no traffic crossing the network segments as they are isolated.
5. Run `y mon` in each network segment to see the activity.

## Bridge evaluation

1. Run `bridge_serial_socketcan.orc.yaml`. It will launch two bridges: `vcan0 <-bridge-> serial <-bridge-> vcan1`.
2. Observe that the subscriber now sees the data from the publisher.
3. Running a new instance of `y sub 1000` or `y pub 1000 whatever` will show that type discovery is also working.

## Router evaluation

1. Run `router_serial_socketcan.orc.yaml`. It will launch two routers: `vcan0 <-router-> serial <-router-> vcan1`.
2. Observe that the subscriber now sees the data from the publisher as if it was published by the nearest bridge
   (which is technically the case).
3. Running a new instance of `y sub 1000` or `y pub 1000 whatever` will fail because the router does not have the
   registers containing the type information, and service transfers are not routed.
