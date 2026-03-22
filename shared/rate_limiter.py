import logging
import time

INTERVAL = 0.5  # seconds

logger = logging.getLogger(__name__)
state = {}


def acquire(key):
    elapsed_time = time.time() - state.get(key, 0)
    if elapsed_time < INTERVAL: 
        wait_time = INTERVAL - elapsed_time
        logger.warning("Rate limit delay %.3fs", wait_time, extra={'key': key})
        time.sleep(wait_time)
    state[key] = time.time()
