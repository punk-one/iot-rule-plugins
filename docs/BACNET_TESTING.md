# BACnet/IP local interoperability tests

Production plugins are pure Go. The independent test server is bacnet-stack
1.7.0, commit 03acade70f4586b4f9aacd77c13d63f6192a021c, built outside this repo.
Do not redistribute this test server as an official plugin or introduce it as
a native runtime dependency.

```bash
git clone --depth 1 --branch bacnet-stack-1.7.0 https://github.com/bacnet-stack/bacnet-stack.git /tmp/bacnet-stack-test
cmake -S /tmp/bacnet-stack-test -B /tmp/bacnet-stack-build -DBACDL_BIP6=OFF -DBACDL_ZIGBEE=OFF -DBAC_ROUTING=OFF
cmake --build /tmp/bacnet-stack-build --target server -j 4
# Requires Linux user/network namespaces, iproute2, Python 3 and the current Go toolchain.
export TEST_BACNET_SERVER=/tmp/bacnet-stack-build/bacserv
unshare -Urn -- bash -c 'ip link set lo up; cd connectors/connect_bacnet; TEST_BACNET_ISOLATED=1 GOWORK=off go test -race -count=1 -v .'
TEST_ENGINE_BINARY=/absolute/path/to/iot-rule-engine ./scripts/test-bacnet-engine.sh
```

All BACnet traffic stays on private loopback in a temporary network namespace.
The wrapper launches a temporary admin/database and only seven required plugin
artifacts. No production PLC, broker, database or credentials are used. Fixture
processes and sockets close when the run exits. Test reports/build logs remain
in the printed temporary directories. Set TEST_OUTPUT_DIR to retain reports in
your designated test workspace. Missing prerequisites fail explicitly; they do
not count as completed interoperability acceptance.

The independent service verifies Device identity, UTF-8 object name, missing
object bad quality and Float32 write/readback. The Engine run additionally
verifies shared resource, two source instances, per-artifact single process,
Sink property acknowledgement and rule-stop isolation. Scalar fixed vectors,
malformed inputs, type boundaries, deadlines and breaker are normal module
tests. This is not BACnet conformance certification, physical hardware testing,
72-hour load testing, or ARM runtime acceptance.
