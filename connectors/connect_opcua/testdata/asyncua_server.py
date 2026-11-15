"""Independent local OPC UA protocol fixture. Never exposed outside loopback."""
import argparse
import asyncio
from importlib.metadata import version
from pathlib import Path
from asyncua import Server, ua

async def run(args):
    if version('asyncua') != '1.1.8':
        raise RuntimeError('this interoperability fixture pins asyncua==1.1.8')
    server = Server()
    await server.init()
    server.set_endpoint(f'opc.tcp://127.0.0.1:{args.port}')
    await server.set_application_uri('urn:iot-rule:server')
    await server.load_certificate(args.cert)
    await server.load_private_key(args.key, format='pem')
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity, ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt])
    if args.extra_namespace:
        await server.register_namespace('urn:iot-rule:unrelated')
    index = await server.register_namespace('urn:iot-rule:test')
    await server.nodes.objects.add_variable(ua.NodeId('counter', index), 'counter', ua.Variant(9007199254740993, ua.VariantType.Int64))
    temperature = await server.nodes.objects.add_variable(ua.NodeId('temperature', index), 'temperature', 12.5)
    await temperature.set_writable()
    generator = await server.get_event_generator(ua.ObjectIds.BaseEventType, server.nodes.server)
    async with server:
        Path(args.ready).write_text(str(index))
        while True:
            await generator.trigger(message='synthetic test event')
            await asyncio.sleep(0.2)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--cert', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--ready', required=True)
    parser.add_argument('--extra-namespace', action='store_true')
    asyncio.run(run(parser.parse_args()))
