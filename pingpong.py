import asyncio
import math

from subsystem import World, Actor


class Ponger(Actor):
    async def on_message(self, msg, sender):
        await asyncio.sleep(1)
        print("pong")
        await self.world.tell(sender, 'ping', self.name)


class Pinger(Actor):
    async def on_message(self, msg, sender):
        # print("msg received", msg, sender)
        await asyncio.sleep(1)
        print("ping")
        await self.world.tell(sender, 'pong', self.name)


async def run(world: World):
    pinger = world.create_actor('pinger', Pinger)
    ponger = world.get_or_create_actor('ponger', Ponger)

    print("sending ping")
    await world.tell('pinger', 'ping', 'ponger')
    print("sent")

if __name__ == '__main__':
    import uvloop
    uvloop.install()

    loop = asyncio.get_event_loop()
    world = World()
    try:
        asyncio.async(run(world))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print('loop.close()')
        loop.run_until_complete(world.destroy())
        loop.close()

