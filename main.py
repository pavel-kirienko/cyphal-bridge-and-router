#!/usr/bin/env python

import asyncio
import logging
import pycyphal  # type: ignore
from pycyphal.transport import Transport, Capture, AlienTransfer, Tracer, TransferTrace, ErrorTrace  # type: ignore
from pycyphal.transport import InputSessionSpecifier, InputSession, MessageDataSpecifier, TransferFrom  # type: ignore
from pycyphal.transport import PayloadMetadata  # type: ignore
import pycyphal.application  # type: ignore
import uavcan.node.port


def main() -> None:
    node_info = pycyphal.application.NodeInfo(name="org.opencyphal.bridge")
    registry = pycyphal.application.make_registry("bridge.db")
    uplink_node = pycyphal.application.make_node(node_info, registry)
    dnlink_node = pycyphal.application.make_node(
        node_info,
        registry,
        transport=_make_dnlink_transport(registry, uplink_node.id),
    )

    _br = Bridge(uplink_node, dnlink_node, queue_capacity=int(registry.setdefault("bridge.queue_capacity", 1000)))

    uplink_node.start()
    dnlink_node.start()

    asyncio.run(asyncio.gather(*asyncio.all_tasks()))


class Bridge:
    """
    Each node has to subscribe to subjects on its own behalf to ensure that relevant traffic is delivered to it
    (e.g., in case of Cyphal/UDP it is necessary to publish IGMP announcements).
    The nodes have to share the same node-ID on both sides.
    """

    EXTENT_BYTES = 1024**2
    """The largest transfer this bridge is able to forward."""

    MIN_OUTPUT_TIMEOUT = 1e-3

    MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD = int(2**48)

    def __init__(
        self,
        up_node: pycyphal.application.Node,
        dn_node: pycyphal.application.Node,
        queue_capacity: int,
    ) -> None:
        loop = asyncio.get_event_loop()

        # Set up subject cross-linking based on the uavcan.node.port.List_0 announcements.
        # Note that we could trivially make these entries expire automatically:
        # just keep a timestamp and update it at every message reception.
        subs_up: dict[int, InputSession] = {}
        subs_dn: dict[int, InputSession] = {}
        up_node.make_subscriber(uavcan.node.port.List_0).receive_in_background(
            lambda msg, _meta: self._handle_uavcan_node_port_list(subs_dn, dn_node, msg)
        )
        dn_node.make_subscriber(uavcan.node.port.List_0).receive_in_background(
            lambda msg, _meta: self._handle_uavcan_node_port_list(subs_up, up_node, msg)
        )

        # The queues hold transfers in transit. They are filled by the snoopers and flushed by the spoofers.
        # TODO: the queues should be prioritized.
        dn2up_queue: asyncio.Queue[TransferTrace] = asyncio.Queue(queue_capacity)  # Filled from the downlink
        up2dn_queue: asyncio.Queue[TransferTrace] = asyncio.Queue(queue_capacity)  # Filled from the uplink

        # Set up transfer snoopers. They deliver all transfers to the queues.
        up_tracer = up_node.presentation.transport.make_tracer()
        dn_tracer = dn_node.presentation.transport.make_tracer()
        up_node.presentation.transport.begin_capture(
            lambda cap: loop.call_soon_threadsafe(self._on_capture, up_tracer, up2dn_queue, cap)  # type: ignore
        )
        dn_node.presentation.transport.begin_capture(
            lambda cap: loop.call_soon_threadsafe(self._on_capture, dn_tracer, dn2up_queue, cap)  # type: ignore
        )

        # Set up spoofing. These task consume data from the queues.
        self._up_spoof = loop.create_task(self._run_spoofing(dn2up_queue, up_node.presentation.transport))
        self._dn_spoof = loop.create_task(self._run_spoofing(up2dn_queue, dn_node.presentation.transport))

    def _handle_uavcan_node_port_list(
        self,
        subscriptions: dict[int, InputSession],
        node: pycyphal.application.Node,
        msg: uavcan.node.port.List_0,
    ) -> None:
        if msg.subscribers.mask:
            for subject_id, used in enumerate(msg.subscribers.mask):
                if used:
                    self._ensure_subscription(subscriptions, node, subject_id)
        elif msg.subscribers.sparse_list:
            for subject_id_obj in msg.subscribers.sparse_list:
                self._ensure_subscription(subscriptions, node, subject_id_obj.value)
        elif msg.subscribers.total:
            for subject_id in range(MessageDataSpecifier.SUBJECT_ID_MASK + 1):
                self._ensure_subscription(subscriptions, node, subject_id)
        else:
            assert False

    @staticmethod
    def _ensure_subscription(
        subscriptions: dict[int, InputSession],
        node: pycyphal.application.Node,
        subject_id: int,
    ) -> None:
        if subject_id in subscriptions:  # We could update the timestamp here to implement automatic expiration.
            return
        # Create the subscription to ensure the lower layers of the network stack are configured to
        # receive the data we need (e.g., IGMP announcements are published, CAN acceptance filters configured, etc).
        subscriptions[subject_id] = node.presentation.transport.get_input_session(
            InputSessionSpecifier(MessageDataSpecifier(subject_id), remote_node_id=None),
            PayloadMetadata(Bridge.EXTENT_BYTES),
        )

    @staticmethod
    def _on_capture(tracer: Tracer, dst: asyncio.Queue[TransferTrace], cap: Capture) -> None:
        res = tracer.update(cap)
        if isinstance(res, TransferTrace):  # A reassembled transfer.
            try:
                dst.put_nowait(res)
            except asyncio.QueueFull:
                _logger.error("Queue full, transfer dropped: %s", res)
        elif isinstance(res, ErrorTrace):
            _logger.warning("Transport-layer error: %s", res)
        else:
            _logger.debug("Unsupported transport event ignored: %s", res)

    @staticmethod
    async def _run_spoofing(src: asyncio.Queue[TransferTrace], dst: Transport) -> None:
        loop = asyncio.get_event_loop()
        max_node_id = dst.protocol_parameters.max_nodes - 1
        while True:
            try:
                item = await src.get()
                if item.transfer.metadata.session_specifier.source_node_id is None:
                    _logger.debug(
                        "Anonymous transfer dropped because the target transport may not support it: %s", item
                    )
                    continue
                if item.transfer.metadata.session_specifier.source_node_id > max_node_id or (
                    item.transfer.metadata.session_specifier.destination_node_id is not None
                    and item.transfer.metadata.session_specifier.destination_node_id > max_node_id
                ):
                    _logger.debug("Transfer not representable on the target network: %s", item)
                    continue
                # Heuristic: transfer-ID timeout is a sensible approximation of the optimal transmission timeout.
                deadline = loop.time() + max(Bridge.MIN_OUTPUT_TIMEOUT, item.transfer_id_timeout)
                # Note that if the transfer-ID of the target transport is cyclic,
                # the modulo will be computed automatically, so we don't have to do anything.
                # This is guaranteed by the PyCyphal API.
                result = await dst.spoof(item.transfer, deadline)
                if not result:
                    _logger.error("Transfer has timed out at output: %s", item)
            except Exception as ex:
                _logger.exception("Spoofing loop error: %s", ex)
                await asyncio.sleep(1.0)


class Tracer:
    def __init__(self) -> None:
        pass


def _make_dnlink_transport(
    registry: pycyphal.application.register.Registry,
    local_node_id: int,
) -> pycyphal.transport.Transport:
    can_iface = str(registry.setdefault("bridge.downlink.can.iface", ""))
    if can_iface:
        can_mtu = int(registry.setdefault("bridge.downlink.can.mtu", 8))
        if can_iface.startswith("socketcan:"):
            from pycyphal.transport.can.media.socketcan import SocketCANMedia  # type: ignore

            media = SocketCANMedia(can_iface.split(":", 1)[1], can_mtu)
        else:
            raise RuntimeError(f"CAN media not supported (yet): {can_iface!r}")
        from pycyphal.transport.can import CANTransport  # type: ignore

        return CANTransport(media, local_node_id=local_node_id)

    raise RuntimeError(f"Downlink transport is not configured or not supported")


_logger = logging.getLogger(__name__)

if __name__ == "__main__":
    main()
