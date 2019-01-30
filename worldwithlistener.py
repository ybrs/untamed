import asyncio
import uvloop
import msgpack

SOCKET_RECV_SIZE = 1024 #** 2


def get_unpacker():
    return msgpack.Unpacker(encoding='utf-8', use_list=False)


async def serve(reader, writer):
    print("1")
    unpacker = get_unpacker()
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

        unpacker.feed(data)
        try:
            req = next(unpacker)
        except msgpack.ExtraData as edata:
            print(edata)
            print("^ --- edata")
            return
        except StopIteration:
            continue
        unpacker = get_unpacker()
        # print("received>>>", req)
        if 'heartbeat' in req:
            # don't ack heartbeats
            writer.write(msgpack.packb({'heartbeat': True}))
            writer.write(b"\r\n")
            continue
        writer.write(msgpack.packb({'acked': True, 'msg_id': req['msg_id']}))
        writer.write(b"\r\n")

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