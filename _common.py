import pycyphal  # type: ignore
import pycyphal.application  # type: ignore


def make_dnlink_transport(
    registry: pycyphal.application.register.Registry,
    register_prefix: str,
    local_node_id: int | None = None,
) -> pycyphal.transport.Transport:
    """
    This is an approximation of pycyphal.application.make_transport().
    """
    can_iface = str(registry.setdefault(register_prefix + ".downlink.can.iface", ""))
    if can_iface:
        can_mtu = int(registry.setdefault(register_prefix + ".downlink.can.mtu", 64))
        if can_iface.startswith("socketcan:"):
            from pycyphal.transport.can.media.socketcan import SocketCANMedia  # type: ignore

            media = SocketCANMedia(can_iface.split(":", 1)[1], can_mtu)
        else:
            raise RuntimeError(f"CAN media not supported (yet): {can_iface!r}")
        from pycyphal.transport.can import CANTransport  # type: ignore

        return CANTransport(media, local_node_id=local_node_id)

    raise RuntimeError(f"Downlink transport is not configured or not supported")
