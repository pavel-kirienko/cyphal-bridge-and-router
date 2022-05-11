#!/usr/bin/env python

import asyncio
import logging
import pycyphal  # type: ignore
from pycyphal.presentation import Presentation  # type: ignore
from pycyphal.transport import Transport, OutputSessionSpecifier, Transfer  # type: ignore
from pycyphal.transport import InputSessionSpecifier, InputSession, PayloadMetadata, TransferFrom  # type: ignore
from pycyphal.transport import MessageDataSpecifier  # type: ignore
import pycyphal.application  # type: ignore
import uavcan.node.port
from _common import make_dnlink_transport


async def main() -> None:
    logging.root.setLevel(logging.DEBUG)
    try:
        import coloredlogs  # type: ignore

        # The level spec applies to the handler, not the root logger! This is different from basicConfig().
        coloredlogs.install(level=logging.DEBUG, fmt=_LOG_FORMAT)
    except Exception as ex:  # pylint: disable=broad-except
        _logger.exception("Could not set up coloredlogs: %r", ex)  # pragma: no cover

    uplink = pycyphal.application.make_node(pycyphal.application.NodeInfo(name="org.opencyphal.router"), "router.db")
    dnlink = _make_dnlink_node(uplink)
    _logger.info("UPLINK:   %s", uplink)
    _logger.info("DOWNLINK: %s", dnlink)
    _br = Router(uplink, dnlink)

    await asyncio.sleep(1e100)


class Router:
    def __init__(self, up: pycyphal.application.Node, dn: pycyphal.application.Node) -> None:
        if up.id is None or dn.id is None:
            raise RuntimeError("A router requires a node-ID, which may be the same on both segments")
        self._extent_bytes = int(
            up.registry.setdefault("router.extent_bytes", pycyphal.application.register.Natural64([1024**2]))
        )
        up.make_subscriber(uavcan.node.port.List_0).receive_in_background(
            lambda msg, _meta: self._handle_uavcan_node_port_list(
                dn.presentation.transport, up.presentation.transport, msg
            )
        )
        dn.make_subscriber(uavcan.node.port.List_0).receive_in_background(
            lambda msg, _meta: self._handle_uavcan_node_port_list(
                up.presentation.transport, dn.presentation.transport, msg
            )
        )
        up.start()
        dn.start()

    def _handle_uavcan_node_port_list(
        self,
        src: Transport,
        dst: Transport,
        msg: uavcan.node.port.List_0,
    ) -> None:
        if msg.subscribers.mask is not None:
            for subject_id, used in enumerate(msg.subscribers.mask):
                if used:
                    self._ensure_subject_forwarder(src, dst, subject_id)
        elif msg.subscribers.sparse_list is not None:
            for subject_id_obj in msg.subscribers.sparse_list:
                self._ensure_subject_forwarder(src, dst, subject_id_obj.value)
        elif msg.subscribers.total is not None:
            _logger.debug("Total subscription ignored, assuming this is a diagnostic tool, not a real node")
        else:
            assert False

    def _ensure_subject_forwarder(self, src: Transport, dst: Transport, subject_id: int) -> None:
        # We could update the timestamp here to implement automatic expiration.
        ds = MessageDataSpecifier(subject_id)
        iss = InputSessionSpecifier(ds, remote_node_id=None)
        oss = OutputSessionSpecifier(ds, remote_node_id=None)
        if iss in {x.specifier for x in src.input_sessions} or oss in {x.specifier for x in dst.output_sessions}:
            return
        loop = asyncio.get_event_loop()
        pm = PayloadMetadata(extent_bytes=self._extent_bytes)
        input_session = src.get_input_session(iss, pm)
        output_session = dst.get_output_session(oss, pm)

        async def task() -> None:
            _logger.info("Setting up forwarder for %s from %s to %s", ds, src, dst)
            # Normally the transfer-ID is managed by the presentation layer in PyCyphal,
            # but as we work below that layer, we have to manage it manually.
            transfer_id = 0
            while True:
                try:
                    match await input_session.receive(loop.time() + 1.0):
                        case Transfer(timestamp=ts, priority=prio, fragmented_payload=fpl):  # type: ignore
                            item = Transfer(
                                timestamp=ts,
                                priority=prio,
                                transfer_id=transfer_id,
                                fragmented_payload=fpl,
                            )
                            _logger.debug("Forwarding %s to %s", item, output_session)
                            transfer_id += 1
                            # TODO transmission timeout auto-tune
                            await output_session.send(item, loop.time() + 1.0)
                        case None:
                            pass
                except pycyphal.transport.ResourceClosedError as ex:
                    _logger.info("Stopping because: %s", ex)

        asyncio.create_task(task())


def _make_dnlink_node(uplink_node: pycyphal.application.Node) -> pycyphal.application.Node:
    tr = make_dnlink_transport(uplink_node.registry, "router", local_node_id=uplink_node.id)
    return pycyphal.application.make_node(
        uplink_node.info,
        registry=uplink_node.registry,
        transport=tr,
    )


_LOG_FORMAT = "%(asctime)s %(process)07d %(levelname)-3.3s %(name)s: %(message)s"
logging.basicConfig(format=_LOG_FORMAT)
_logger = logging.getLogger(__name__)

if __name__ == "__main__":
    asyncio.run(main())
