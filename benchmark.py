import argparse
import time
from redis import Redis
from redis.client import Pipeline

redis_client = Redis(host = '127.0.0.1', port = 6379, db = 0)


def set(n):
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.set("key-"+str(i), str(i))
    end_time = time.perf_counter()
    print("Time taken for set = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def pipe_set(n):
    pipe = redis_client.pipeline()
    start_time = time.perf_counter()
    for i in range(n) :
        pipe.set("key-"+str(i), str(i))
    pipe.execute()
    end_time = time.perf_counter()
    print("Time taken for set (bulk) = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def get(n):
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.get("key-"+str(i))
    end_time = time.perf_counter()
    print("Time taken for get = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def pipe_get(n):
    pipe = redis_client.pipeline()
    start_time = time.perf_counter()
    for i in range(n) :
        pipe.get("key-"+str(i))
    pipe.execute()
    end_time = time.perf_counter()
    print("Time taken for get (bulk) = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def delete(n):
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.delete("key-"+str(i))
    end_time = time.perf_counter()
    print("Time taken for del = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def pipe_delete(n):
    pipe = redis_client.pipeline()
    start_time = time.perf_counter()
    for i in range(n) :
        pipe.delete("key-"+str(i))
    pipe.execute()
    end_time = time.perf_counter()
    print("Time taken for del (bulk) = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")


def hSet(n):
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.hset("hash-key", "key-"+str(i), str(i))
    end_time = time.perf_counter()
    print("Time taken for hSet = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def pipe_hSet(n):
    pipe = redis_client.pipeline()
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.hset("hash-key", "key-"+str(i), str(i))
    pipe.execute()
    end_time = time.perf_counter()
    print("Time taken for hSet (bulk) = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def hGet(n):
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.hget("hash-key", "key-"+str(i))
    end_time = time.perf_counter()
    print("Time taken for hGet = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def pipe_hGet(n):
    pipe = redis_client.pipeline()
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.hget("hash-key", "key-"+str(i))
    pipe.execute()
    end_time = time.perf_counter()
    print("Time taken for hGet (bulk) = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def hDel(n):
    start_time = time.perf_counter()
    for i in range(n) :
        redis_client.delete("hash-key", "key-"+str(i))
    end_time = time.perf_counter()
    print("Time taken for hDel = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def pipe_hDel(n):
    pipe = redis_client.pipeline()
    start_time = time.perf_counter()
    for i in range(n) :
        pipe.delete("hash-key", "key-"+str(i))
    pipe.execute()
    end_time = time.perf_counter()
    print("Time taken for hDel (bulk) = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def mSet(n):
    start_time = time.perf_counter()
    items = {}
    for i in range(n) :
        items["key-"+str(i)] = str(i)
    redis_client.mset(items)
    end_time = time.perf_counter()
    print("Time taken for mSet = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def mGet(n):
    start_time = time.perf_counter()
    keys = []
    for i in range(n) :
        keys.append("key-"+str(i))
    redis_client.mget(keys)
    end_time = time.perf_counter()
    print("Time taken for mGet = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def hmSet(n):
    start_time = time.perf_counter()
    items = {}
    for i in range(n) :
        items["key-"+str(i)] = str(i)
    redis_client.hmset("hash-key", items)
    end_time = time.perf_counter()
    print("Time taken for hmSet = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")

def hmGet(n):
    start_time = time.perf_counter()
    keys = []
    for i in range(n) :
        keys.append("key-"+str(i))
    redis_client.hmget("hash-key", keys)
    end_time = time.perf_counter()
    print("Time taken for hmGet = " + str(round(1000 * (end_time - start_time))) + "ms (for " + str(n) + " requests)\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to anyalyse performance testing on Redis Commands")
    parser.add_argument("-n", "--num", help="Specify the number of requests")

    args = parser.parse_args()

    print(args.num)

    req_count = int(args.num)

    set(req_count)
    get(req_count)
    delete(req_count)
    pipe_set(req_count)
    pipe_get(req_count)
    pipe_delete(req_count)
    hSet(req_count)
    hGet(req_count)
    hDel(req_count)
    pipe_hSet(req_count)
    pipe_hGet(req_count)
    pipe_hDel(req_count)
    mSet(req_count)
    mGet(req_count)
    hmSet(req_count)
    mGet(req_count)
