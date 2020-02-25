import asyncio
import websockets
import time

class Periodic:
    def __init__(self, ws):
        self.ws = ws

    async def loop(self):
        while True:
            await self.ws.send(f"pong - {time.time()}")
            await asyncio.sleep(1)


async def hello():
    uri = "ws://localhost:9111/ws/room1"
    async with websockets.connect(uri) as websocket:
        await websocket.send("hello")
        p = Periodic(websocket)
        asyncio.ensure_future(p.loop())

        while True:
            greeting = await websocket.recv()
            print(f"< {greeting}")


asyncio.get_event_loop().run_until_complete(hello())
