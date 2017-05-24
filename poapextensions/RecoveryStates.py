import logging
from threading import Lock
import pickle

# Get module-level logger
logger = logging.getLogger(__name__)

"""
State and Lock primitives for use with recoverable workers.
Access to recovery state objects must be modulated by
recovery lock objects in order to maintain state integrity.
"""


class BasicLock(object):
    """
    Lock primitive for use with recoverable workers.
    This implementation simply applies the recovery lock
    interface to the threading.Lock module.

    Interface:

    (1) __init__(self, record_id) : BasicLock

    (2) acquire(self, [blocking]) : boolean
        Acquire the lock. @param blocking is optional.

    (3) release(self) : unit

    (4) cleanup(self) : unit
        Called on evaluation completion, interrupted or otherwise.
    """

    def __init__(self, record_id):
        self._lock = Lock()

    def acquire(self, blocking=True):
        return self._lock.acquire(blocking)

    def release(self):
        return self._lock.release()

    def cleanup(self):
        return


class BasicStateObject(dict):
    """
    State primitive for use with recoverable workers.
    This implementation simply applies the recovery state
    interface to the Python dictionary type.

    Interface:

    (1) __init__(self, record_id, [sourceStr]) : BasicLock
        if sourceStr is not None, the state should initialize
        based on the information in sourceStr, such as pickled
        information or the location of a saved state file.

    (2) cleanup(self) : unit
        Called on evaluation completion, interrupted or otherwise.

    (3) save(self) : (boolean, [String])
        Using information only internal to itself, the state
        should save itself to some recoverable form.
        If saving is successful, the second location in the
        returned tuple must not be present. It contains information
        on how to recover the state. This value will be fed to
        state.__init__.
    """

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
