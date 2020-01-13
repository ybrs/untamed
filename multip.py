from multiprocessing import Process, Pipe
import ujson
import time


def f(conn):
    while True:
        msg = conn.recv()
        print("->", msg)
        msg = ujson.loads(msg)
        conn.send(msg['msg_id'])
    conn.close()


if __name__ == '__main__':
    parent_conn, child_conn = Pipe()
    p = Process(target=f, args=(child_conn,))
    p.start()
    t1 = time.time()

    waitlist = set()
    n = 500
    for i in range(n):
        req = {
            'msg_id': i,
            'msg': 'hello {}'.format(i),
            'from': ['foo', 'bar']
        }
        print(i)
        parent_conn.send(ujson.dumps(req))
        waitlist.add(i)

    t = time.time() - t1
    print("0->", t, n/t)

    while waitlist:
        i = parent_conn.recv()
        print(i)
        waitlist.remove(i)
    t = time.time() - t1
    print("->", t, n/t)

    print("done")

    p.join()