import asyncio
import time
from functools import partial

from sanic import Sanic
from sanic.response import json

from untamed import SuspendableActor, Actor


class WebListenerActor(Actor):
    def __init__(self, app, *args, **kw):
        self.app = app
        super(WebListenerActor, self).__init__(*args, **kw)

    async def after_create(self):
        server = self.app.create_server(
            host="0.0.0.0", port=9111, return_asyncio_server=True
        )
        asyncio.ensure_future(server)


class SomeActor(SuspendableActor):
    async def on_message(self, msg, sender):
        await super().on_message(msg, sender)

        if msg.get("cmd", None) == "set_foo":
            await self.set_state(msg["data"])

        if msg.get("cmd", None) == "get_foo":
            await self.world.tell(
                sender, {"reply_to": msg["msg_id"], "data": self.state}
            )


app = Sanic()


@app.route("/")
async def test(request):
    world = request.ctx.world
    t = time.time()

    await world.tell("some-actor", {"cmd": "set_foo", "data": {"t": t}})
    result = await world.tell_and_get("some-actor", {"cmd": "get_foo"})

    return json({"actors": world.actors.keys(), "reply": result})


@app.websocket("/ws")
async def feed(request, ws):
    while True:
        data = "hello!"
        print("Sending: " + data)
        await ws.send(data)
        data = await ws.recv()
        print("Received: " + data)


def web_server(app, *args, **kw):
    return WebListenerActor(app, *args, **kw)


async def main(world):
    await world.basic_config()
    actor = world.create_actor("some-actor", SomeActor)

    @app.middleware("request")
    async def add_world(request):
        request.ctx.world = world

    webserv = world.create_actor("web-server", partial(web_server, app))
