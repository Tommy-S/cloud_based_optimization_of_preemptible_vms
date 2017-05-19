import logging
from threading import Lock
import traceback

TIMEOUT = 0
# Get module-level logger
logger = logging.getLogger(__name__)


class RecoverablePreemptibleMixIn(object):
    def __init__(self, workerClass, lockClass, stateClass):
        self.workerClass = workerClass
        self.lockClass = lockClass
        self.stateClass = stateClass

    def handle_eval(self, *params):
        stateLock = self.lockClass()
        state = self.stateClass()
        self.workerClass.handle_eval(self, *(params + (stateLock, state)))
        stateLock.cleanup()
        state.cleanup()

    def finish_preempted(self, *params):
        saved = False
        stateLock = params[-2]
        state = params[-1]

        try:
            if stateLock.acquire(False):
                (savedSuccessfully, savedStateInfo) = state.save()
                if savedSuccessfully:
                    saved = True
        except Exception:
            logger.error(traceback.format_exc())

        if saved:
            self.finish_preempted_state_saved(savedStateInfo, *params)
        else:
            self.finish_preempted_state_unsaved(*params)

    def finish_preempted_state_saved(self, savedStateInfo, *params):
        logger.debug("Preempt with state saved")
        self.workerClass.finish_preempted(self, *params)

    def finish_preempted_state_unsaved(self, *params):
        logger.debug("Preempt with state unsaved")
        self.workerClass.finish_preempted(self, *params)

    def handle_kill(self, *params):
        self.workerClass.handle_kill(*params[:-2])


class BasicLock(object):
    def __init__(self):
        self._lock = Lock()

    def acquire(self, blocking=True):
        return self._lock.acquire(blocking)

    def release(self):
        return self._lock.release()

    def cleanup(self):
        return


class BasicStateObject(dict):
    def __init__(self, sourceStr=None):
        dict.__init__(self)

    def cleanup(self):
        return

    def save(self):
        logger.info(self)
        return (True, None)
