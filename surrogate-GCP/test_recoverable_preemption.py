from preemptibleWorkers import PreemptibleBasicWorkerThread
from preemptibleControllers import PreemptibleThreadController
import logging
from recoverablePreemptibleMixIn import RecoverablePreemptibleMixIn, BasicLock, BasicStateObject
import traceback
from preemptibleWorkers import PreemptibleSimpleSocketWorker
from preemptibleControllers import PreemptibleThreadedTCPServer
import threading
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


class TestRecoverablePreemptibleSimpleSocketWorker(RecoverablePreemptibleMixIn, PreemptibleSimpleSocketWorker):
    """
    Add a controllable preemption system to a PreemptibleBasicWorkerThread.
    This is only for unit-testing purposes.
    """

    def __init__(self, objective, sockname, retries=0, threadID=0):
        self.id = threadID
        RecoverablePreemptibleMixIn.__init__(self, PreemptibleSimpleSocketWorker, BasicLock, BasicStateObject)
        PreemptibleSimpleSocketWorker.__init__(self, objective, sockname, retries)

    def is_preempted(self):
        return self.id == 0

    def eval(self, stateLock, state, record_id, params):
        logging.debug('Worker {0} starting eval'.format(self.id))
        time.sleep(1)
        if self.id == 1:
            self.id = 0

        try:
            args = (stateLock, state) + params
            msg = ('complete', record_id, self.objective(*args))
            logging.debug("Worker finished feval successfully")
        except Exception:
            logging.debug("Worker feval exited with exception")
            logging.error(traceback.format_exc())
            msg = ('cancel', record_id)
        return (self.send,) + msg


def testRecoverablePreemptibleTCPServer():
    """Testing routine."""
    logging.info('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: PreemptibleTCPServer >>>>>>>>>>>>>>>>>>>\n\n')
    # Launch controller
    samples = [0.0, 0.1]
    strategy = FixedSampleStrategy(samples)
    server = PreemptibleThreadedTCPServer(strategy=strategy)
    cthread = threading.Thread(target=server.run)
    cthread.start()

    # Get controller port
    name = server.sockname
    logging.info("Launch controller at {0}".format(name))

    # Launch workers
    def worker_main(name, threadID):
        logging.info("Launching worker on port {0}".format(name[1]))
        TestRecoverablePreemptibleSimpleSocketWorker(objective, sockname=name, retries=1, threadID=threadID).run()

    wthreads = []
    for threadID in range(4):
        wthread = threading.Thread(target=worker_main, args=(name, threadID))
        wthread.start()
        wthreads.append(wthread)

    # Wait on controller and workers
    cthread.join()
    for t in wthreads:
        t.join()

    result = server.controller.best_point()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.DEBUG)
    # testBasicRecovery()
    testRecoverablePreemptibleTCPServer()

if __name__ == '__main__':
    main()
