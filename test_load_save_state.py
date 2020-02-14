import asyncio
import time

import pytest

from subsystem import World, SuspendableActor, RedisPersistence


class SomeActor(SuspendableActor):
    async def on_message(self, msg, sender):
        await super().on_message(msg, sender)
        print("sender!!!!", sender, msg)

        if msg.get('cmd', None) == 'set_foo':
            await self.set_state(msg['data'])

        if msg.get('cmd', None) == 'get_foo':
            await self.world.tell(sender, {'reply_to': msg['msg_id'], 'data': self.state})


@pytest.mark.asyncio
async def test_load_save():
    world = World()
    persistence = world.create_actor('persistence', RedisPersistence)
    await persistence.tell({'cmd': 'connect', 'data': 'redis://localhost:6379/12'})
    print("--- actors::", world.actors)
    actor = world.create_actor('some-actor', SomeActor)

    t = time.time()
    await world.tell('some-actor', {'cmd': 'set_foo', 'data': {'t': t}})
    result = await world.tell_and_get('some-actor', {'cmd': 'get_foo'})
    assert result['data']['t'] == t
    print("--- actors::", world.actors)

    await world.suspend_actor('some-actor')
    print("--- actors::", world.actors)

    await world.revive_actor('some-actor', SomeActor)
    result = await world.tell_and_get('some-actor', {'cmd': 'get_foo'})
    assert result['data']['t'] == t

    # await world.stop_actor('persistence')
    # await world.stop_actor('some-actor')
    # await world.stop_actor('world')
    await world.stop()
