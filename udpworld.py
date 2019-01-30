import asyncio
import uvloop
import msgpack
import ujson

def get_unpacker():
    return msgpack.Unpacker(encoding='utf-8', use_list=False)


class EchoServerProtocol:
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # message = data.decode()
        msg = msgpack.unpackb(data, encoding='utf-8')
        # print('Received %r from %s' % (msg, addr))
        ackmsg = msgpack.packb({'ack': msg['msg_id']})
        # print('Send %r to %s' % ('ack', addr))
        self.transport.sendto(ackmsg, addr)
        # print("->", msg['msg_id'])


async def main():
    print("Starting UDP server")

    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_event_loop()

    # One protocol instance will be created to serve all
    # client requests.
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: EchoServerProtocol(),
        local_addr=('127.0.0.1', 9999))

    try:
        await asyncio.sleep(3600)  # Serve for 1 hour.
    finally:
        transport.close()


if __name__ == '__main__':
    uvloop.install()

    loop = asyncio.get_event_loop()
    # coro = asyncio.start_server(main, '127.0.0.1', 6000, loop=loop)

    server = loop.run_until_complete(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        pass
        # server.close()
        # loop.run_until_complete(server.wait_closed())