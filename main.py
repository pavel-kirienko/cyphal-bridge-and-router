#!/usr/bin/env python

import asyncio
import logging
from typing import Callable
import pycyphal  # type: ignore
from pycyphal.transport import Transport, Capture, Tracer, TransferTrace, ErrorTrace  # type: ignore
from pycyphal.transport import InputSessionSpecifier, InputSession, PayloadMetadata, TransferFrom  # type: ignore
from pycyphal.transport import MessageDataSpecifier, ServiceDataSpecifier, SessionSpecifier  # type: ignore
from pycyphal.transport import AlienTransfer, AlienTransferMetadata, AlienSessionSpecifier  # type: ignore
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
        up_tr = up_node.presentation.transport
        dn_tr = dn_node.presentation.transport

        up_tid_rect = self._make_transfer_id_rectifier(up_tr)
        dn_tid_rect = self._make_transfer_id_rectifier(dn_tr)

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
        up_tracer = up_tr.make_tracer()
        dn_tracer = dn_tr.make_tracer()
        up_tr.begin_capture(
            lambda cap: loop.call_soon_threadsafe(  # type: ignore
                self._on_capture,
                up_tracer,
                up2dn_queue,
                up_tid_rect(cap),
            )
        )
        dn_tr.begin_capture(
            lambda cap: loop.call_soon_threadsafe(  # type: ignore
                self._on_capture,
                dn_tracer,
                dn2up_queue,
                dn_tid_rect(cap),
            )
        )

        # Set up spoofing. These task consume data from the queues.
        self._up_spoof = loop.create_task(self._run_spoofing(dn2up_queue, up_tr))
        self._dn_spoof = loop.create_task(self._run_spoofing(up2dn_queue, dn_tr))

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

    @staticmethod
    def _make_transfer_id_rectifier(tr: Transport) -> Callable[[TransferTrace], TransferTrace]:
        par = tr.protocol_parameters
        if par.transfer_id_modulo < Bridge.MONOTONIC_TRANSFER_ID_MODULO_THRESHOLD:
            rect = TransferIDRectifier(par.max_nodes, par.transfer_id_modulo)

            def impl(tt: TransferTrace) -> TransferTrace:
                # Looks wild, huh? The dataclasses used in PyCyphal are immutable, but we need to change one field
                # deep inside the transfer trace event data. So we have to decompose everything and then put it back
                # together again. We could pull use some immutability framework but it's not worth it here.
                return TransferTrace(
                    timestamp=tt.timestamp,
                    transfer=AlienTransfer(
                        metadata=AlienTransferMetadata(
                            priority=tt.transfer.metadata.priority,
                            transfer_id=rect.rectify(
                                SessionSpecifier(
                                    data_specifier=tt.transfer.metadata.session_specifier.data_specifier,
                                    remote_node_id=tt.transfer.metadata.session_specifier.source_node_id,
                                ),
                                tt.transfer.metadata.transfer_id,
                            ),
                            session_specifier=tt.transfer.metadata.session_specifier,
                        ),
                        fragmented_payload=tt.transfer.fragmented_payload,
                    ),
                    transfer_id_timeout=tt.transfer_id_timeout,
                )

            return impl

        return lambda obj: obj


class TransferIDRectifier:
    """
    Unwraps cyclic transfer-IDs making them monotonic.
    This allows bridging transports with cyclic/monotonic transfer-IDs in both directions.
    Doing so requires keeping 64 bits per (port, node);
    for CAN, this requires (8192 + 512*2) * 128 nodes * 8 bytes = 9 MiB of RAM.
    Notice that the service-ID space is multiplied by two to account for requests and responses.
    """

    _NUM_SUBJECTS = MessageDataSpecifier.SUBJECT_ID_MASK + 1
    _NUM_SERVICES = ServiceDataSpecifier.SERVICE_ID_MASK + 1

    def __init__(self, max_nodes: int, transfer_id_modulo: int) -> None:
        self._num_node_ids = max_nodes
        self._mod = transfer_id_modulo
        self._table = [0] * (self._NUM_SUBJECTS + self._NUM_SERVICES * 2) * self._num_node_ids

    def rectify(self, ss: SessionSpecifier, tid: int) -> int:
        if ss.remote_node_id is None:
            # Anonymous transfers do not really have a well-defined transfer-ID, so don't bother.
            # Cyphal does not handle them differently but here it allows us to save memory.
            return tid
        if tid >= self._mod:
            raise ValueError(f"Transfer-ID shall be less than its modulo: {tid}<{self._mod}")
        idx = self._compute_index(ss)
        self._table[idx] += self._compute_forward_distance((self._table[idx] % self._mod), tid)
        return self._table[idx]

    def _compute_index(self, ss: SessionSpecifier) -> int:
        dim2_cardinality = self._num_node_ids
        ds, dim2 = ss.data_specifier, ss.remote_node_id
        assert dim2 is not None, "Transfer-ID rectification is not defined for anonymous transfers"
        if isinstance(ds, MessageDataSpecifier):
            dim1 = ds.subject_id
        elif isinstance(ds, ServiceDataSpecifier):
            if ds.role == ds.Role.REQUEST:
                dim1 = ds.service_id + self._NUM_SUBJECTS
            elif ds.role == ds.Role.RESPONSE:
                dim1 = ds.service_id + self._NUM_SUBJECTS + self._NUM_SERVICES
            else:
                assert False
        else:
            assert False
        return dim1 * dim2_cardinality + dim2

    def _compute_forward_distance(self, a: int, b: int) -> int:
        """From the Cyphal/CAN bus transport layer specification."""
        assert a >= 0 and b >= 0
        a %= self._mod
        b %= self._mod
        d = b - a
        if d < 0:
            d += self._mod
        assert 0 <= d < self._mod
        assert (a + d) & (self._mod - 1) == b
        return d


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
