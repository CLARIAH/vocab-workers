import redis

from vocab.config import redis_uri

r = redis.Redis.from_url(redis_uri)


def store_object_redis(nr: int, id: int, obj: bytes):
    r.set('{}:{}'.format(nr, id), obj)


def get_object_redis(nr: int, id: int) -> bytes:
    return r.get('{}:{}'.format(nr, id))


def delete_object_redis(nr: int, id: int):
    r.delete('{}:{}'.format(nr, id))
