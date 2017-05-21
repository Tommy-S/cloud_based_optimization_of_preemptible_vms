import logging
from threading import Lock
import pickle

TIMEOUT = 0
# Get module-level logger
logger = logging.getLogger(__name__)


class BasicLock(object):
    def __init__(self, record_id):
        self._lock = Lock()

    def acquire(self, blocking=True):
        return self._lock.acquire(blocking)

    def release(self):
        return self._lock.release()

    def cleanup(self):
        return


class BasicStateObject(dict):
    def __init__(self, record_id, sourceStr=None):
        if sourceStr is not None:
            dict.__init__(self, pickle.loads(sourceStr))
        else:
            dict.__init__(self)

    def cleanup(self):
        return

    def save(self):
        dct = {}
        for key, value in self.iteritems():
            dct[key] = value
        logger.info(dct)
        return (True, pickle.dumps(dct))
