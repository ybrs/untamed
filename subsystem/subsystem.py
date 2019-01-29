import asyncio
import random


class Actor(object):
    def __init__(self, name, queue, world):
        self.queue = queue
        self.name = name
        self.world = world

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
            # print(">>>", item)
            await self.on_message(*item)


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
        await actor.queue.put((msg, sender))

    async def suspend_actor(self, name):
        actor = self.actors[name]
        await actor.queue.put(None)
        del self.actors[name]

    async def destroy(self):
        for k, actor in self.actors.items():
            await actor.queue.put(None)


