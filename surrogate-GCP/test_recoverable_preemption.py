from preemptibleWorkers import PreemptibleBasicWorkerThread
from preemptibleControllers import PreemptibleThreadController
import logging
from recoverablePreemptibleMixIn import RecoverablePreemptibleMixIn, BasicLock, BasicStateObject
import traceback
"""
Test fixed sampling strategy.
"""

import time
from poap.strategy import FixedSampleStrategy
from poap.strategy import CheckWorkerStrategy
from poap.test.monitor import add_monitor


def objective(stateLock, state, x):
    def initState():
        if 'count' not in state:
            stateLock.acquire()
            state['count'] = 0
            stateLock.release()
        else:
            logging.info("Initialized with count of {0}".format(state['count']))
    logging.info("Request for {0}".format(x))
    initState()

    while state['count'] < 5:
        logging.debug("Count is {0}".format(state['count']))
        time.sleep(0.33)
        stateLock.acquire()
        state['count'] = state['count'] + 1
        stateLock.release()

    return (x + 1)


class TestBasicRecoverablePreemptibleWorker(RecoverablePreemptibleMixIn, PreemptibleBasicWorkerThread):
    """
    Add a controllable preemption system to a PreemptibleBasicWorkerThread.
    This is only for unit-testing purposes.
    """

    def __init__(self, controller, objective, threadID=0):
        self.id = threadID
        RecoverablePreemptibleMixIn.__init__(self, PreemptibleBasicWorkerThread, BasicLock, BasicStateObject)
        PreemptibleBasicWorkerThread.__init__(self, controller, objective)

    def is_preempted(self):
        return self.id == 0

    def eval(self, stateLock, state, record):
        if self.id == 1:
            self.id = 0
        try:
            args = (stateLock, state) + record.params
            value = self.objective(*args)
            logging.debug("Worker finished feval successfully")
            return [self.finish_success, record, value]
        except Exception:
            logging.debug("Worker feval exited with exception")
            logging.error(traceback.format_exc())
            return [self.finish_cancelled, record]


def testBasicRecovery():
    """Testing routine."""
    logging.info('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: PreemptibleBasicWorkerThread >>>>>>>>>>>>>>>>>>>\n\n')
    samples = [0.0, 0.1]
    controller = PreemptibleThreadController()
    strategy = FixedSampleStrategy(samples)
    strategy = CheckWorkerStrategy(controller, strategy)
    controller.strategy = strategy
    add_monitor(controller, 1)

    for threadID in range(4):
        """
        Worker 0 will be pre-empted as soon as it is launched.
        Worker 1 will be pre-empted as soon as an eval is launched.
        Workers 2 and 3 will execute the requests.
        """
        controller.launch_worker(TestBasicRecoverablePreemptibleWorker(controller, objective, threadID))

    result = controller.run()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.DEBUG)
    testBasicRecovery()

if __name__ == '__main__':
    main()
