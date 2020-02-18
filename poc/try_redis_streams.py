import asyncio
import aioredis
import functools

loop = asyncio.get_event_loop()




async def loop_():
    while True:
        print("hello")
        await asyncio.sleep(1)


class Reader:
    def __init__(self, redis):
        self.redis = redis

    async def another(self):
        # redis = await aioredis.create_redis('redis://localhost', loop=loop)

        while True:
            result = await self.redis.xread(["foobar2"])
            print(result)


async def main():
    streams = ['foobar', 'anotherstream']
    redis = await aioredis.create_redis('redis://localhost', loop=loop)
    asyncio.ensure_future(loop_())

    r = Reader(redis)
    asyncio.ensure_future(r.another())


    while True:
        result = await redis.xread(streams)
        print(result)

    redis.close()
    await redis.wait_closed()

loop.run_until_complete(main())

