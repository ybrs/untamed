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

    async def pre_message(self, msg, sender):
        return msg, sender

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
                item = await self.pre_message(*item)
                if item:
                    await self.on_message(*item)
            except Exception as e:
                logger.exception(f"Exception on processing on_message {self.name}")
                print("-> exception", e, type(e), self, item)
        # print("Exiting actor")


class SuspendableActor(Actor):
    def __init__(self, name, queue, world):
        self.state = {}
        self.wait_for = None
        self.wait_queue = []
        super().__init__(name, queue, world)

    async def set_state(self, new_state: dict):
        self.state.update(new_state)

    async def save_state(self):
        logger.info("saving state 2")

        await self.world.tell('persistence', {'cmd': 'SAVE_STATE', 'data': self.state}, sender=self.name)

    async def load_state(self):
        await self.world.tell('persistence', {'cmd': 'LOAD_STATE'}, sender=self.name)

    async def replay_waiting_messages(self):
        for t in self.wait_queue:
            item = await self.pre_message(*t)
            if item:
                await self.on_message(*item)

    async def pre_message(self, msg, sender):
        if self.wait_for:
            if msg['cmd'] != self.wait_for:
                logger.info(f"waiting for {self.wait_for} but received {msg}")
                self.wait_queue.append((msg, sender))
            else:
                logger.info(f"resolved waiting for {self.wait_for}")
                asyncio.ensure_future(self.replay_waiting_messages())
                self.wait_for = None
                return msg, sender
        else:
            return msg, sender

    async def on_message(self, msg, sender):
        if msg['cmd'] == 'INTERNAL_SUSPEND':
            logger.info("saving state ")

            await self.save_state()
            return

        if msg['cmd'] == 'INTERNAL_RELOAD_STATE':
            logger.info("re-loading state ")
            await self.load_state()
            self.wait_for = 'LOADED_STATE'
            return

        if msg['cmd'] == 'LOADED_STATE':
            return await self.set_state(msg['data'])

        try:
            await self.set_state({'recv': self.state.get('recv', 0) + 1})
        except Exception:  # noqa
            logger.exception("Exception received on message in actor")

        await super().on_message(msg, sender)


class RedisPersistence(Actor):
    async def save(self, key, value):
        return await self.redis.set(key, pickle.dumps(value))

    async def load(self, key, sender):
        data = await self.redis.get(key)
        data = pickle.loads(data)
        logger.info("loaded data - %s", data)
        await self.world.tell(sender, {'cmd': 'LOADED_STATE', 'data': data})

    async def connect(self, conn_url):
        self.redis = await aioredis.create_redis_pool(conn_url)
        await self.redis.ping()

    async def on_stop(self):
        self.redis.close()
        await self.redis.wait_closed()

    async def on_message(self, msg, sender):
        # logger.info("PERSISTENCE - got message on persistence %s %s", msg, sender)
        if msg['cmd'] == 'connect':
            await self.connect(msg['data'])

        if msg['cmd'] == 'SAVE_STATE':
            await self.save(f'state::{sender}', msg['data'])

        if msg['cmd'] == 'LOAD_STATE':
            logger.info("load sate !")
            await self.load(f'state::{sender}', sender)

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

    async def tell(self, who, msg, sender: str = None):
        logger.info("to actor - %s %s %s", who, msg, sender)
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
        await actor.tell({'cmd': 'INTERNAL_RELOAD_STATE'})

    async def destroy(self):
        for k, actor in self.actors.items():
            await actor.queue.put(None)
