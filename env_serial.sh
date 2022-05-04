export UAVCAN__SERIAL__IFACE='socket://192.168.1.200:50905'
export UAVCAN__NODE__ID=$(python -c 'print(__import__("random").getrandbits(7))')
echo "Auto-selected node-ID for this session: $UAVCAN__NODE__ID"
