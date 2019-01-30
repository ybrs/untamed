import asyncio
import msgpack
import uvloop
import time
import pickle
from worldwithlistener import serve
import ujson as json

"""
notes - this can do around 14k per second.

(venv3) $ python worldclient.py 127.0.0.1:6000
time -  0.25844502449035645 193464.74206109426  per second
periodic
13
completed 3.4832000732421875
time -  3.4832000732421875 14354.616142810208  per second

# buffered write
time -  0.4210240840911865 118758.05182957387  per second

"""


def get_unpacker():
    return msgpack.Unpacker(encoding='utf-8', use_list=False)


class Writer(object):
    def __init__(self, host, port, writer):
        self.counter = 0
        self.host = host
        self.port = port
        self.writer = writer
        self.buffer = []

    def write_buf(self):
        packed = json.dumps(self.buffer).encode()
        self.writer.write(packed)
        self.writer.write(b'\r\n')
        self.buffer = []

    def write(self, msg):
        self.counter += 1
        req = {
            'msg_id': self.counter,
            'msg': msg,
            'from': [self.host, self.port]
        }
        self.buffer.append(req)
        if len(self.buffer) == 100:
            self.write_buf()
        return self.counter


async def tcp_echo_client(host, port, server_host, server_port):

    async def heartbeat(writer):
        while True:
            print('periodic')
            packed = json.dumps({'heartbeat': time.time()})

            writer.write(packed.encode())
            writer.write(b'\r\n')
            await asyncio.sleep(10)

    reader, writer = await asyncio.open_connection(
        host, int(port))

    print(writer.transport.get_write_buffer_limits())

    enchanced_writer = Writer(server_host, server_port, writer)
    # enchanced_writer.write({'connected': True})

    asyncio.ensure_future(heartbeat(writer))

    waiting = set()
    t1 = time.time()
    n = 50000
    for i in range(1, n):
        waiting.add(enchanced_writer.write({'foo': 'bar'}))
    enchanced_writer.write_buf()
    diff = time.time() - t1
    print("time - ", diff, n/diff, " per second")

    # print(waiting)
    # unpacker = get_unpacker()
    while True:
        data = await asyncio.wait_for(reader.readuntil(b'\r\n'), 30)
        # print("->", data)
        if not data:
            return

        reqs = json.loads(data)
        for req in reqs:
            try:
                if 'heartbeat' in req:
                    continue
            except:
                print(req)
                # raise
                continue
            waiting.remove(req['msg_id'])
            # print(waiting)
            if not waiting:
                diff = time.time() - t1
                print("completed", diff)
                print("time - ", diff, n / diff, " per second")


            # print('Close the connection')
    # writer.close()

    # await writer.wait_closed()

import sys


async def main(server_host, server_port):
    seed_node, seed_port = sys.argv[1].split(':')
    await tcp_echo_client(seed_node, seed_port, server_host, server_port)

loop = uvloop.new_event_loop()
coro = asyncio.start_server(serve, '127.0.0.1', None, loop=loop)
server = loop.run_until_complete(coro)

server_host, server_port = server.sockets[0].getsockname()

asyncio.set_event_loop(loop)
loop.run_until_complete(main(server_host, server_port))
