import time

import pytest

from subsystem import World, SuspendableActor

@pytest.mark.asyncio
async def test_load_save():
    world = World()
    actor = world.create_actor('some-actor', SuspendableActor)
    await world.suspend_actor('some-actor')
    time.sleep(1)
    await world.revive_actor('some-actor')

