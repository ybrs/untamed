import asyncio
import pickle
import random
import logging
import uuid
import aioredis

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Actor:
    def __init__(self, name, queue, world):
        self.queue = queue
        self.name = name
        self.world = world

    async def tell(self, msg, sender=None, msg_id=None):
        if msg_id:
            msg['msg_id'] = msg_id
        await self.queue.put((msg, sender))

    async def on_message(self, msg, sender):
        """\
        override this

        :param msg:
        :param sender:
        :return:
        """
        pass

    async def on_stop(self):
        pass

    async def consume(self):
        while True:
            # wait for an item from the producer
            # item is (msg, sender) tuple
            item = await self.queue.get()
            if item is None:
                # the producer emits None to indicate that it is done
                # print("breaking - {}".format(self.name))
                await self.on_stop()
                break
            try:
                await self.on_message(*item)
            except Exception as e:
                print("-> exception", e, type(e), self, item)
        # print("Exiting actor")


class SuspendableActor(Actor):
    def __init__(self, name, queue, world):
        self.state = {}
        super().__init__(name, queue, world)

    async def set_state(self, new_state: dict):
        self.state.update(new_state)

    async def save_state(self):
        await self.world.tell('persistence', {'cmd': 'SAVE_STATE', 'data': self.state})

    async def load_state(self):
        await self.world.tell('persistence', {'cmd': 'LOAD_STATE'})

    async def on_message(self, msg, sender):
        if msg['cmd'] == 'INTERNAL_SUSPEND':
            await self.save_state()
            return

        if msg['cmd'] == 'INTERNAL_REVIVE':
            await self.load_state()
            return

        try:
            await self.set_state({'recv': self.state.get('recv', 0) + 1})
        except Exception:  # noqa
            logger.exception("Exception received on message in actor")

        await super().on_message(msg, sender)


class RedisPersistence(Actor):
    async def save(self, key, value):
        return await self.redis.set(key, pickle.dumps(value))

    async def load(self, key):
        return await self.redis.get(key)

    async def connect(self, conn_url):
        self.redis = await aioredis.create_redis_pool(conn_url)
        await self.redis.ping()

    async def on_stop(self):
        self.redis.close()
        await self.redis.wait_closed()

    async def on_message(self, msg, sender):
        try:
            if msg['cmd'] == 'connect':
                await self.connect(msg['data'])
        except Exception as e:
            print("Exc on p", e)

    def __del__(self):
        print("delete !")

class WorldActor(Actor):
    async def on_message(self, msg, sender):
        try:
            await super().on_message(msg, sender)
            print("msg world actor", msg)
            if 'reply_to' in msg:
                await self.world.got_reply(msg)
        except Exception as e:
            print("-> exc world actor", e, type(e))
            logger.exception("x")

class World:
    def __init__(self):
        self.actors = {}
        self.self_actor = self.create_actor('world', WorldActor)
        self.wait_reply_list = {}

    def create_actor(self, name, klass=Actor, **init):
        print("create actor", name)
        queue = asyncio.Queue()
        actor = klass(name, queue, self, **init)
        self.actors[name] = actor
        asyncio.ensure_future(actor.consume())
        return actor

    def get_actor(self, name):
        actor = self.actors.get(name, None)
        if not actor:
            print("actorssss", self.actors)
            raise Exception(f'Actor "{name}" not found ')
        return actor

    def get_or_create_actor(self, name, klass=Actor):
        actor = self.actors.get(name, None)
        if actor:
            return actor
        return self.create_actor(name, klass)

    async def tell(self, who, msg, sender=None):
        actor = self.get_actor(who)
        # print("found actor", actor)
        await actor.tell(msg, sender)

    async def tell_and_get(self, who, msg, sender=None):
        actor = self.get_actor(who)
        msg_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.wait_reply_list[msg_id] = fut

        await actor.tell(msg, sender=self.self_actor.name, msg_id=msg_id)
        print("---->", fut, type(fut))

        return await fut

    async def got_reply(self, msg):
        print("got reply !!!!", msg)
        future = self.wait_reply_list[msg['reply_to']]
        future.set_result(msg)

    async def suspend_actor(self, name):
        print("suspend actor", name)
        actor = self.actors[name]
        await actor.tell({'cmd': 'INTERNAL_SUSPEND'}, None)
        await actor.queue.put(None)
        del self.actors[name]

    async def stop_actor(self, name):
        print("stop actor - ", name)
        actor = self.actors[name]
        await actor.queue.put(None)
        del self.actors[name]

    async def revive_actor(self, name, klass):
        actor = self.create_actor(name, klass)
        await self.tell(name, {'cmd': 'INTERNAL_RELOAD_STATE'})

    async def destroy(self):
        for k, actor in self.actors.items():
            await actor.queue.put(None)
