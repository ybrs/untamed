import asyncio
import uvloop
import msgpack
import pickle
import ujson as json

SOCKET_RECV_SIZE = 1024 #** 2


def get_unpacker():
    return msgpack.Unpacker(encoding='utf-8', use_list=False)


class BufferedReplyWriter(object):
    def __init__(self, writer):
        self.buffer = []
        self.writer = writer

    def write_buf(self):
        packed = json.dumps(self.buffer).encode()  # msgpack.packb(req)
        self.writer.write(packed)
        self.writer.write(b'\r\n')
        self.buffer = []

    def write(self, req):
        self.buffer.append(req)
        if len(self.buffer) == 5:
            self.write_buf()


async def serve(reader, writer):
    buffered_reply_writer = BufferedReplyWriter(writer)

    while True:
        # TODO: we'll expect a heartbeat or disconnect after 30 secs
        try:
            data = await asyncio.wait_for(reader.readuntil(b'\r\n'), 30)
        except:
            print("connection ended - quitting")
            return
        # data = reader.read(SOCKET_RECV_SIZE)
        # print("data -", data)
        if not data:
            return

        # unpacker.feed(data)
        # try:
        #     req = next(unpacker)
        # except msgpack.ExtraData as edata:
        #     print(edata)
        #     print("^ --- edata")
        #     return
        # except StopIteration:
        #     continue
        # unpacker = get_unpacker()
        # print("received>>>", req)
        try:
            reqs = json.loads(data)
        except:
            print("-----------------")
            print(data)
            print("//---------------")
            continue
        # print("received>>>", req)
        for req in reqs:
            # print(req)
            if 'heartbeat' in req:
                # don't ack heartbeats
                writer.write(json.dumps({'heartbeat': True}).encode())
                writer.write(b"\r\n")
                continue
            buffered_reply_writer.write({'acked': True, 'msg_id': req['msg_id']})
        buffered_reply_writer.write_buf()

if __name__ == '__main__':
    uvloop.install()

    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(serve, '127.0.0.1', 6000, loop=loop)

    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())