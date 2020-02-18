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
        self.stopping = False
        asyncio.ensure_future(self.loop())

    async def loop(self):
        pass

    async def tell(self, msg, sender=None, msg_id=None):
        if msg_id:
            msg["msg_id"] = msg_id
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

    async def post_stop(self):
        await self.world.tell(
            self.world.self_actor.name, {"cmd": "STOPPED"}, sender=self.name
        )

    async def pre_message(self, msg, sender):
        return msg, sender

    async def consume(self):
        while True:
            # wait for an item from the producer
            # item is (msg, sender) tuple
            item = await self.queue.get()
            logger.info(f"consume {item}")
            if item is None:
                # the producer emits None to indicate that it is done
                # print("breaking - {}".format(self.name))
                self.stopping = True
                await self.on_stop()
                break

            if item and "cmd" in item[0]:
                if item[0]["cmd"] == "INTERNAL_STOP":
                    self.stopping = True
                    await self.on_stop()
                    await self.post_stop()
                    return

            try:
                item = await self.pre_message(*item)
                if item:
                    await self.on_message(*item)
            except Exception as e:
                logger.exception(f"Exception on processing on_message {self.name}")
                print("-> exception", e, type(e), self, item)


class SuspendableActor(Actor):
    def __init__(self, name, queue, world):
        self.state = {}
        self.wait_for = None
        self.wait_queue = []
        super().__init__(name, queue, world)

    async def set_state(self, new_state: dict):
        self.state.update(new_state)

    async def save_state(self):
        await self.world.tell(
            "persistence", {"cmd": "SAVE_STATE", "data": self.state}, sender=self.name
        )

    async def load_state(self):
        await self.world.tell("persistence", {"cmd": "LOAD_STATE"}, sender=self.name)

    async def replay_waiting_messages(self):
        for t in self.wait_queue:
            item = await self.pre_message(*t)
            if item:
                await self.on_message(*item)

    async def pre_message(self, msg, sender):
        if self.wait_for:
            if msg["cmd"] != self.wait_for:
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
        if msg["cmd"] == "INTERNAL_SUSPEND":
            logger.info("saving state ")

            await self.save_state()
            return

        if msg["cmd"] == "INTERNAL_RELOAD_STATE":
            logger.info("re-loading state ")
            await self.load_state()
            self.wait_for = "LOADED_STATE"
            return

        if msg["cmd"] == "LOADED_STATE":
            return await self.set_state(msg["data"])

        try:
            await self.set_state({"recv": self.state.get("recv", 0) + 1})
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
        await self.world.tell(sender, {"cmd": "LOADED_STATE", "data": data})

    async def connect(self, conn_url):
        self.redis = await aioredis.create_redis_pool(conn_url)
        await self.redis.ping()

    async def on_stop(self):
        self.redis.close()
        await self.redis.wait_closed()

    async def on_message(self, msg, sender):
        # logger.info("PERSISTENCE - got message on persistence %s %s", msg, sender)
        if msg["cmd"] == "connect":
            await self.connect(msg["data"])

        if msg["cmd"] == "SAVE_STATE":
            await self.save(f"state::{sender}", msg["data"])

        if msg["cmd"] == "LOAD_STATE":
            logger.info("load sate !")
            await self.load(f"state::{sender}", sender)

    def __del__(self):
        print("delete !")


class WorldActor(Actor):
    async def on_message(self, msg, sender):
        try:
            await super().on_message(msg, sender)
            logger.info("msg world actor %s", msg)
            if "reply_to" in msg:
                await self.world.got_reply(msg)

            if msg.get("cmd", None) == "STOPPED":
                logger.info(f"received STOPPED {sender}")
                if sender in self.world.wait_stop:
                    self.world.wait_stop.remove(sender)
                    del self.world.actors[sender]

        except Exception as e:
            logger.exception("exception on world actor")
            print("-> exc world actor", e, type(e))


class TaskScheduler(SuspendableActor):
    async def on_message(self, msg, sender):
        await super().on_message(msg, sender)
        if msg["cmd"] == "schedule":
            await self.set_state(
                {
                    msg["data"]["at"]: {
                        "at": msg["data"]["at"],
                        "actor": msg["data"]["actor"],
                        "msg": msg["data"]["msg"],
                    }
                }
            )

    async def loop(self):
        while True:
            if self.stopping:
                return
            await asyncio.sleep(0.100)


class World:
    def __init__(self):
        self.actors = {}
        self.self_actor = self.create_actor("world", WorldActor)
        self.task_scheduler = self.create_actor("task_scheduler", TaskScheduler)
        self.wait_reply_list = {}
        self.wait_stop = set()

    def create_actor(self, name, klass=Actor, **init):
        queue = asyncio.Queue()
        actor = klass(name, queue, self, **init)
        self.actors[name] = actor
        asyncio.ensure_future(actor.consume())
        return actor

    def get_actor(self, name):
        actor = self.actors.get(name, None)
        if not actor:
            logger.warning(f'Actor "{name}" not found')
        return actor

    def get_or_create_actor(self, name, klass=Actor):
        actor = self.actors.get(name, None)
        if actor:
            return actor
        return self.create_actor(name, klass)

    async def tell(self, who, msg, sender: str = None):
        logger.info("to actor - %s %s %s", who, msg, sender)
        actor = self.get_actor(who)
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
        future = self.wait_reply_list[msg["reply_to"]]
        future.set_result(msg)

    async def suspend_actor(self, name):
        actor = self.actors[name]
        await actor.tell({"cmd": "INTERNAL_SUSPEND"}, None)
        await actor.queue.put(None)
        del self.actors[name]

    async def remove_actor(self, name):
        actor = self.actors[name]
        await actor.queue.put(None)
        del self.actors[name]

    async def stop_actor(self, name):
        actor = self.get_actor(name)
        self.wait_stop.add(name)
        logger.info(f"WAIT self.wait_stop {self.wait_stop}")
        await actor.tell({"cmd": "INTERNAL_STOP"}, None)
        while True:
            if not self.wait_stop or (name not in self.wait_stop):
                break
            await asyncio.sleep(0.100)

    async def stop(self):
        for name, actor in self.actors.items():
            if name != self.self_actor.name:
                self.wait_stop.add(name)
                logger.info(f"WAIT self.wait_stop {self.wait_stop}")
                await actor.tell({"cmd": "INTERNAL_STOP"}, None)

        while True:
            if not self.wait_stop:
                break
            await asyncio.sleep(0.100)

        await self.remove_actor(self.self_actor.name)

    async def revive_actor(self, name, klass):
        actor = self.create_actor(name, klass)
        await actor.tell({"cmd": "INTERNAL_RELOAD_STATE"})

    async def destroy(self):
        for k, actor in self.actors.items():
            await actor.queue.put(None)


async def run(world):
    persistence = world.create_actor('persistence', RedisPersistence)
    await persistence.tell({'cmd': 'connect', 'data': 'redis://localhost:6379/11'})
    # await world.stop_actor('persistence')


def run_world():
    import uvloop
    uvloop.install()

    loop = asyncio.get_event_loop()
    world = World()
    try:
        asyncio.ensure_future(run(world))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print('step: loop.close()')
        loop.run_until_complete(world.stop())
        loop.close()
