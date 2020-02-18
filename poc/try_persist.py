import asyncio
import time

from subsystem import World, RedisPersistence, SuspendableActor


async def run(world: World, n):
    persistence = world.create_actor('persistence', RedisPersistence)
    await persistence.tell({'cmd': 'connect', 'data': 'redis://localhost:6379/11'})
    # actor = world.create_actor('some-actor', SuspendableActor)
    # await world.suspend_actor('some-actor')
    # time.sleep(1)
    # await world.revive_actor('some-actor')
    await world.stop_actor('persistence')



if __name__ == '__main__':
    import uvloop
    uvloop.install()

    loop = asyncio.get_event_loop()
    world = World()
    try:
        asyncio.ensure_future(run(world, 100_000))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print('step: loop.close()')
        loop.run_until_complete(world.destroy())
        loop.close()
