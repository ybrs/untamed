import asyncio
import random
import logging

logger = logging.getLogger(__name__)


class Actor(object):
    def __init__(self, name, queue, world):
        self.queue = queue
        self.name = name
        self.world = world

    async def tell(self, msg, sender=None):
        await self.queue.put((msg, sender))

    async def on_message(self, msg, sender):
        """\
        override this

        :param msg:
        :param sender:
        :return:
        """
        pass

    async def consume(self):
        while True:
            # wait for an item from the producer
            item = await self.queue.get()
            if item is None:
                # the producer emits None to indicate that it is done
                # print("breaking - {}".format(self.name))
                break
            await self.on_message(*item)


class SuspendableActor(Actor):
    def __init__(self, name, queue, world):
        self.state = {}
        super().__init__(name, queue, world)

    def set_state(self, new_state: dict):
        self.state.update(new_state)

    async def save_state(self):
        await self.world.tell(self.name, {'cmd': 'SAVE_STATE', 'data': self.state})

    async def load_state(self):
        await self.world.tell(self.name, {'cmd': 'LOAD_STATE'})

    async def on_message(self, msg, sender):
        if msg == 'INTERNAL_SUSPEND':
            await self.save_state()
            return

        if msg == 'INTERNAL_REVIVE':
            await self.load_state()
            return

        try:
            self.set_state({'recv': self.state.get('recv', 0) + 1})
        except Exception:  # noqa
            logger.exception("Exception received on message in actor")

        await super().on_message(msg, sender)


class World(object):
    def __init__(self):
        self.actors = {}

    def create_actor(self, name, klass=Actor):
        queue = asyncio.Queue()
        actor = klass(name, queue, self)
        self.actors[name] = actor
        asyncio.ensure_future(actor.consume())
        return actor

    def get_actor(self, name):
        return self.actors[name]

    def get_or_create_actor(self, name, klass=Actor):
        actor = self.actors.get(name, None)
        if actor:
            return actor
        return self.create_actor(name, klass)

    async def tell(self, who, msg, sender=None):
        actor = self.get_actor(who)
        # print("found actor", actor)
        await actor.tell(msg, sender)

    async def suspend_actor(self, name):
        actor = self.actors[name]
        await actor.tell('INTERNAL_SUSPEND', None)
        await actor.queue.put(None)
        del self.actors[name]

    async def revive_actor(self, name):
        pass

    async def destroy(self):
        for k, actor in self.actors.items():
            await actor.queue.put(None)
