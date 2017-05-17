from surrogateGCP.poapextensions.SimpleWorkers import (
    SimpleEventThreadWorker,
    SimpleInterruptibleThreadWorker,
    SimpleEventSocketWorker,
    SimpleInterruptibleSocketWorker,
)
from poap.tcpserve import ThreadedTCPServer
import threading
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


def testSimpleEventThreadWorker():
    """Testing routine."""
    print('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: SimpleEventThreadWorker >>>>>>>>>>>>>>>>>>>')
    samples = [0.0, 0.1]
    controller = ThreadController()
    strategy = FixedSampleStrategy(samples)
    strategy = CheckWorkerStrategy(controller, strategy)
    controller.strategy = strategy
    add_monitor(controller, 1)

    for threadID in range(2):
        controller.launch_worker(SimpleEventThreadWorker(controller, objective))

    result = controller.run()
    value = result.value
    if value == 1:
        print("Test OK")
    else:
        print("Test Failed:")
        print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def testSimpleInterruptibleThreadWorker():
    """Testing routine."""
    print('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: SimpleInterruptibleThreadWorker >>>>>>>>>>>>>>>>>>>')
    samples = [0.0, 0.1]
    controller = ThreadController()
    strategy = FixedSampleStrategy(samples)
    strategy = CheckWorkerStrategy(controller, strategy)
    controller.strategy = strategy
    add_monitor(controller, 1)

    for threadID in range(2):
        controller.launch_worker(SimpleInterruptibleThreadWorker(controller, objective))

    result = controller.run()
    value = result.value
    if value == 1:
        print("Test OK")
    else:
        print("Test Failed:")
        print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def testSimpleEventSocketWorker():
    """Testing routine."""
    logging.info('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: SimpleEventSocketWorker >>>>>>>>>>>>>>>>>>>\n\n')
    # Launch controller
    samples = [0.0]
    strategy = FixedSampleStrategy(samples)
    server = ThreadedTCPServer(strategy=strategy)
    cthread = threading.Thread(target=server.run)
    cthread.start()

    # Get controller port
    name = server.sockname
    logging.info("Launch controller at {0}".format(name))

    # Launch workers
    def worker_main(name, threadID):
        logging.info("Launching worker on port {0}".format(name[1]))
        SimpleEventSocketWorker(objective, sockname=name, retries=1).run()

    wthreads = []
    for threadID in range(1):
        wthread = threading.Thread(target=worker_main, args=(name, threadID))
        wthread.start()
        wthreads.append(wthread)

    # Wait on controller and workers
    cthread.join()
    for t in wthreads:
        t.join()

    result = server.controller.best_point()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def testSimpleInterruptibleSocketWorker():
    """Testing routine."""
    logging.info('\n\n<<<<<<<<<<<<<<<<<<<< Starting test: SimpleInterruptibleSocketWorker >>>>>>>>>>>>>>>>>>>\n\n')
    # Launch controller
    samples = [0.0]
    strategy = FixedSampleStrategy(samples)
    server = ThreadedTCPServer(strategy=strategy)
    cthread = threading.Thread(target=server.run)
    cthread.start()

    # Get controller port
    name = server.sockname
    logging.info("Launch controller at {0}".format(name))

    # Launch workers
    def worker_main(name, threadID):
        logging.info("Launching worker on port {0}".format(name[1]))
        SimpleInterruptibleSocketWorker(objective, sockname=name, retries=1).run()

    wthreads = []
    for threadID in range(1):
        wthread = threading.Thread(target=worker_main, args=(name, threadID))
        wthread.start()
        wthreads.append(wthread)

    # Wait on controller and workers
    cthread.join()
    for t in wthreads:
        t.join()

    result = server.controller.best_point()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def main(args):
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.INFO)

    if len(args) > 0 and (args[0] == 's' or args[0] == 'silent'):
        logging.disable(logging.CRITICAL)

    testSimpleEventThreadWorker()
    testSimpleInterruptibleThreadWorker()
    testSimpleEventSocketWorker()
    testSimpleInterruptibleSocketWorker()

if __name__ == '__main__':
    main(sys.argv[1:])
