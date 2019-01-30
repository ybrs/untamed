import asyncio
import uvloop
import msgpack
import time
import ujson as json

"""
notes this can do around 25k/sec - but packet loss over 5k
completed 0.16845202445983887 for 5000

"""


class EchoClientProtocol:
    def __init__(self, loop):
        self.loop = loop
        self.transport = None
        self.on_con_lost = loop.create_future()
        self.cnt = 0
        self.waiting = set()
        self.t1 = 0

    def connection_made(self, transport):
        self.transport = transport
        print('Connection made:')
        # self.transport.sendto(self.message.encode())
        self.t1 = time.time()

    def datagram_received(self, data, addr):
        # print("Received:", data.decode())
        msg = msgpack.unpackb(data, encoding='utf-8')
        # msg = json.decode(data)
        # print("received", msg)
        self.waiting.remove(msg['ack'])
        # print("Close the socket")
        # self.transport.close()
        # print(msg['ack'])
        if not self.waiting:
            print("completed", time.time() - self.t1)

    def write(self, msg):
        self.cnt += 1
        self.waiting.add(self.cnt)
        req = {
            'msg_id': self.cnt,
            'msg': msg
        }
        self.transport.sendto(msgpack.packb(req))
        # self.transport.sendto(json.encode(req))

    def error_received(self, exc):
        print('Error received:', exc)

    def connection_lost(self, exc):
        print("Connection closed")
        self.on_con_lost.set_result(True)


async def main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_event_loop()

    p = EchoClientProtocol(loop)
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: p,
        remote_addr=('127.0.0.1', 9999))

    for i in range(1, 5000):
        p.write('hello {}'.format(i))

    try:
        await protocol.on_con_lost
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