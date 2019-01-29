import asyncio
import math

from subsystem import World


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])


def print_process_memory():
    import os
    import psutil
    process = psutil.Process(os.getpid())
    print(convert_size(process.memory_info().rss))


async def run(world: World, n):
    import time
    t1 = time.time()
    for x in range(1, n+1):
        world.create_actor('worker_{}'.format(x))
    print("time for {} - {} elapsed".format(n, time.time() - t1))
    print_process_memory()
    print("slleep 10")
    await asyncio.sleep(3)
    print("done ")

    for x in range(1, n + 1):
        actor = world.get_actor('worker_{}'.format(x))
        print('producing {}/{}'.format(x, n))
        item = str(x)
        await actor.queue.put((item, None))

    print("slleep 10")
    await asyncio.sleep(3)
    print("done ")

    print_process_memory()
    print("suspend")
    for x in range(1, n + 1):
        # print("suspending", x)
        await world.suspend_actor('worker_{}'.format(x))

    while True:
        print_process_memory()
        print("sleep")
        await asyncio.sleep(1)
        print("sleep")


if __name__ == '__main__':
    import uvloop
    uvloop.install()

    loop = asyncio.get_event_loop()
    world = World()
    try:
        asyncio.async(run(world, 100000))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print('step: loop.close()')
        loop.run_until_complete(world.destroy())
        loop.close()

"""
(venv3) $ python subsystem.py
time for 10000 - 0.1893923282623291 elapsed
35.19 MB
^Cstep: loop.close()
(venv3) $ python subsystem.py
time for 100000 - 2.4515750408172607 elapsed
234.11 MB
^Cstep: loop.close()
(venv3) $ python subsystem.py
time for 1000000 - 25.982571840286255 elapsed
2.15 GB
^Cstep: loop.close()
"""
