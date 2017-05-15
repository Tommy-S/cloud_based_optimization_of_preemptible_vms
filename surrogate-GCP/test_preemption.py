from preemptibleWorkers import PreemptibleBasicWorkerThread, PreemptibleSimpleSocketWorker
from preemptibleControllers import PreemptibleThreadController, PreemptibleThreadedTCPServer
import threading
import logging
"""
Test fixed sampling strategy.
"""

import time
import random
from poap.strategy import FixedSampleStrategy
from poap.strategy import CheckWorkerStrategy
from poap.test.monitor import add_monitor


def objective(x):
    """Objective function -- run for about five seconds before returning."""
    logging.info("Request for {0}".format(x))
    time.sleep(2 + random.random())
    return (x + 1)


class TestPreemptibleBasicWorkerThread(PreemptibleBasicWorkerThread):
    """
    Add a controllable preemption system to a PreemptibleBasicWorkerThread.
    This is only for unit-testing purposes.
    """

    def __init__(self, controller, objective, threadID=0):
        self.id = threadID
        PreemptibleBasicWorkerThread.__init__(self, controller, objective)

    def is_preempted(self):
        return self.id == 0

    def eval(self, record):
        if self.id == 1:
            self.id = 0
        return PreemptibleBasicWorkerThread.eval(self, record)


def testPreemptibleBasicWorkerThread():
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
        controller.launch_worker(TestPreemptibleBasicWorkerThread(controller, objective, threadID))

    result = controller.run()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


class TestPreemptibleSimpleSocketWorker(PreemptibleSimpleSocketWorker):
    """
    Add a controllable preemption system to a PreemptibleBasicWorkerThread.
    This is only for unit-testing purposes.
    """

    def __init__(self, objective, sockname, retries=0, threadID=0):
        self.id = threadID
        PreemptibleSimpleSocketWorker.__init__(self, objective, sockname, retries)

    def is_preempted(self):
        return self.id == 0

    def eval(self, record_id, params):
        logging.debug('Worker {0} starting eval'.format(self.id))
        time.sleep(1)
        if self.id == 1:
            self.id = 0
        return PreemptibleSimpleSocketWorker.eval(self, record_id, params)


def testPreemptibleTCPServer():
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
        TestPreemptibleSimpleSocketWorker(objective, sockname=name, retries=1, threadID=threadID).run()

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
    # testPreemptibleBasicWorkerThread()
    testPreemptibleTCPServer()

if __name__ == '__main__':
    main()
