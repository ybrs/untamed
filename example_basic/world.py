import asyncio
import time
from functools import partial

from sanic import Sanic
from sanic.response import json
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

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


class WebsocketConnectionActor(Actor):
    def __init__(self, ws, *args, **kw):
        self.ws = ws
        super(WebsocketConnectionActor, self).__init__(*args, **kw)


class Pinger:
    def __init__(self, ws):
        self.ws = ws

    async def loop(self):
        while True:
            try:
                await self.ws.send("ping")
                await asyncio.sleep(1)
            except (ConnectionClosed, ConnectionClosedError):
                return

@app.websocket("/ws/<room>")
async def websocket(request, ws, room):
    p = Pinger(ws)
    asyncio.ensure_future(p.loop())

    while True:
        try:
            data = await ws.recv()
        except (ConnectionClosed, ConnectionClosedError):
            print("CONNECTION CLOSED")
            return
        except Exception as e:
            print("EXCEPTION.........", e, type(e), ws.state)
            return
        await ws.send(f"welcome to {room} - {data}")


def web_server(app, *args, **kw):
    return WebListenerActor(app, *args, **kw)


async def main(world):
    await world.basic_config()

    @app.middleware("request")
    async def add_world(request):
        request.ctx.world = world

    app.static('/static', './static')

    webserv = world.create_actor("web-server", partial(web_server, app))
    actor = world.create_actor("some-actor", SomeActor)
