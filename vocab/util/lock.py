# Inspiration and code from the following sources:
# https://docs.celeryq.dev/en/latest/tutorials/task-cookbook.html#ensuring-a-task-is-only-executed-one-at-a-time
# https://gist.github.com/aaronpolhamus/cb305a3350f943215d00b66c85f576ea
# https://redis-py.readthedocs.io/en/stable/lock.html

import uuid
import base64
import logging

from redis import StrictRedis
from contextlib import contextmanager

from vocab.config import redis_uri

log = logging.getLogger(__name__)

rds = StrictRedis(redis_uri, decode_responses=True, charset="utf-8")

REMOVE_ONLY_IF_OWNER_SCRIPT = """
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end
"""


@contextmanager
def redis_lock(lock_name: str, expires: int = 60):
    random_value = str(uuid.uuid4())
    lock_acquired = bool(
        rds.set(lock_name, random_value, ex=expires, nx=True)
    )
    log.debug(f'Lock acquired? {lock_name} for {expires} - {lock_acquired}')

    yield lock_acquired

    if lock_acquired:
        # if lock was acquired, then try to release it BUT ONLY if we are the owner
        # (i.e. value inside is identical to what we put there originally)
        rds.eval(REMOVE_ONLY_IF_OWNER_SCRIPT, 1, lock_name, random_value)
        log.debug(f'Lock {lock_name} released!')


def argument_signature(*args, **kwargs):
    arg_list = [str(x) for x in args]
    kwarg_list = [f"{str(k)}:{str(v)}" for k, v in kwargs.items()]
    return base64.b64encode(f"{'_'.join(arg_list)}-{'_'.join(kwarg_list)}".encode()).decode()


def task_lock(func=None, main_key="", timeout=None):
    def _dec(run_func):
        def _caller(*args, **kwargs):
            with redis_lock(f"{main_key}_{argument_signature(*args, **kwargs)}", timeout) as acquired:
                if not acquired:
                    return "Task execution skipped -- another task already has the lock"
                return run_func(*args, **kwargs)

        return _caller

    return _dec(func) if func is not None else _dec
