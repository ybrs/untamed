from sanic import Sanic
from sanic.response import json, text


import multiprocessing
import os
import time

# the_queue = multiprocessing.Queue()
#
#
# def worker_main(queue):
#     while True:
#         item = queue.get(True)

from subsystem import World, Actor

app = Sanic()


class Worker(Actor):
    async def on_message(self, msg, sender):
        # print(msg)
        pass

world = World()


@app.route('/')
async def test(request):
    # the_queue.put_nowait("hello")
    world.get_or_create_actor('worker', Worker)
    await world.tell('worker', 'hello', None)
    return text('hello') # json({'hello': 'world'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, access_log=False)

"""
$ wrk -d 10 -c 10 http://localhost:8080/
Running 10s test @ http://localhost:8080/
  2 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   784.20us  161.65us   4.42ms   91.85%
    Req/Sec     6.26k   352.64     6.88k    79.21%
  125759 requests in 10.10s, 15.23MB read
Requests/sec:  12451.77
Transfer/sec:      1.51MB

-- without actors
$ wrk -d 10 -c 10 http://localhost:8080/
Running 10s test @ http://localhost:8080/
  2 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   711.47us  253.82us  14.30ms   94.63%
    Req/Sec     6.94k   535.72     7.76k    83.66%
  139591 requests in 10.10s, 16.91MB read
Requests/sec:  13821.06
Transfer/sec:      1.67MB

"""