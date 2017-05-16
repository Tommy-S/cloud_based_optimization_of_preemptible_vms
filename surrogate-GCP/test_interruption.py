from interruptibleWorkers import BasicEventWorkerThread, BasicInterruptibleWorkerThread
import logging
import sys
"""
Test fixed sampling strategy.
"""

import time
import random
from poap.strategy import FixedSampleStrategy
from poap.strategy import CheckWorkerStrategy
from poap.test.monitor import add_monitor
from poap.controller import ThreadController

logger = logging.getLogger(__name__)


def objective(x):
    """Objective function -- run for about five seconds before returning."""
    logger.info("Request for {0}".format(x))
    time.sleep(2 + random.random())
    return (x + 1)


def testBasicEventWorkerThread():
    """Testing routine."""
    print('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: BasicEventWorkerThread >>>>>>>>>>>>>>>>>>>')
    samples = [0.0, 0.1]
    controller = ThreadController()
    strategy = FixedSampleStrategy(samples)
    strategy = CheckWorkerStrategy(controller, strategy)
    controller.strategy = strategy
    add_monitor(controller, 1)

    for threadID in range(2):
        controller.launch_worker(BasicEventWorkerThread(controller, objective))

    result = controller.run()
    value = result.value
    if value == 1:
        print("Test OK")
    else:
        print("Test Failed:")
        print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def testBasicInterruptibleWorkerThread():
    """Testing routine."""
    print('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: BasicInterruptibleWorkerThread >>>>>>>>>>>>>>>>>>>')
    samples = [0.0, 0.1]
    controller = ThreadController()
    strategy = FixedSampleStrategy(samples)
    strategy = CheckWorkerStrategy(controller, strategy)
    controller.strategy = strategy
    add_monitor(controller, 1)

    for threadID in range(2):
        controller.launch_worker(BasicInterruptibleWorkerThread(controller, objective))

    result = controller.run()
    value = result.value
    if value == 1:
        print("Test OK")
    else:
        print("Test Failed:")
        print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def main(args):
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.DEBUG)

    if len(args) > 0 and (args[0] == 's' or args[0] == 'silent'):
        logging.disable(logging.CRITICAL)

    # testBasicEventWorkerThread()
    testBasicInterruptibleWorkerThread()

if __name__ == '__main__':
    main(sys.argv[1:])
