from preemptibleWorkers2 import SimplePreemptibleWorkerThread
from preemptibleControllers import PreemptibleThreadController
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


class TestSimplePreemptibleWorkerThread(SimplePreemptibleWorkerThread):
    """
    Add a controllable preemption system to a PreemptibleBasicWorkerThread.
    This is only for unit-testing purposes.
    """

    def __init__(self, controller, objective, threadID=0):
        SimplePreemptibleWorkerThread.__init__(self, controller, objective)
        self.id = threadID

    def is_preempted(self):
        return self.id == 0

    def evaluate(self, record):
        if self.id == 1:
            self.id = 0
        return SimplePreemptibleWorkerThread.evaluate(self, record)


def testSimplePreemptibleWorkerThread():
    """Testing routine."""
    logging.info('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: SimplePreemptibleWorkerThread >>>>>>>>>>>>>>>>>>>\n\n')
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
        controller.launch_worker(TestSimplePreemptibleWorkerThread(controller, objective, threadID))

    result = controller.run()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.DEBUG)
    testSimplePreemptibleWorkerThread()

if __name__ == '__main__':
    main()
