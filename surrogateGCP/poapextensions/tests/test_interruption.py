from surrogateGCP.poapextensions.SimpleWorkers import (
    SimpleEventThreadWorker,
    SimpleInterruptibleThreadWorker,
    SimplePreemptibleThreadWorker,
    SimpleEventSocketWorker,
    SimpleInterruptibleSocketWorker,
    SimplePreemptibleSocketWorker,
)
from surrogateGCP.poapextensions.preemptibleControllers import PreemptibleThreadedTCPServer
from poap.tcpserve import ThreadedTCPServer
import threading
import logging
import sys
"""
Test fixed sampling strategy.
"""

import socket
import errno
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

simple_thread_workers = (
    SimpleEventThreadWorker,
    SimpleInterruptibleThreadWorker,
    SimplePreemptibleThreadWorker,
)

simple_socket_workers = (
    SimpleEventSocketWorker,
    SimpleInterruptibleSocketWorker,
    SimplePreemptibleSocketWorker,
)


def testSimpleThreadWorkerEvaluation(threadWorker):
    """Testing routine."""
    print('<<<<<<<<<<<<<<<<<<<<  Testing Evaluation on {0}  >>>>>>>>>>>>>>>>>>>'.format(threadWorker.__name__))
    samples = [0.0]
    controller = ThreadController()
    strategy = FixedSampleStrategy(samples)
    strategy = CheckWorkerStrategy(controller, strategy)
    controller.strategy = strategy
    add_monitor(controller, 1)

    for threadID in range(1):
        controller.launch_worker(SimpleEventThreadWorker(controller, objective))

    result = controller.run()
    if result.value == 1:
        print("Test Passed")
    else:
        print("Test Failed:")
        print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def testSimpleSocketWorkerEvaluation(socketWorker):
    """Testing routine."""
    print('<<<<<<<<<<<<<<<<<<<<  Testing Evaluation on {0}  >>>>>>>>>>>>>>>>>>>'.format(socketWorker.__name__))
    # Launch controller
    samples = [0.0]
    strategy = FixedSampleStrategy(samples)
    hostip = socket.gethostbyname(socket.gethostname())

    port = 50000
    portopen = False
    while not portopen and port < 60000:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((hostip, port))
            s.close()
            portopen = True
            logger.debug("Port open")
        except socket.error as error:
            if not error.errno == errno.EADDRINUSE:
                raise
            else:
                logger.debug("Port closed")
                port += 1
    name = (hostip, port)
    server = PreemptibleThreadedTCPServer(sockname=name, strategy=strategy)
    cthread = threading.Thread(target=server.run, name='Server for {0}'.format(socketWorker.__name__))
    cthread.start()

    # Get controller port
    name = server.sockname
    logging.debug("Launch controller at {0}".format(name))

    # Launch workers
    def worker_main(name, threadID):
        logging.debug("Launching worker on port {0}".format(name[1]))
        socketWorker(objective, sockname=name, retries=1).run()

    wthreads = []
    for threadID in range(1):
        wthread = threading.Thread(target=worker_main, args=(name, threadID), name='SocketWorker')
        print(wthread.name)
        wthread.start()
        wthreads.append(wthread)

    # Wait on controller and workers
    cthread.join()
    for t in wthreads:
        t.join()

    result = server.controller.best_point()
    if result.value == 1:
        print("Test Passed")
    else:
        print("Test Failed:")
        print("Final: {0:.3e} @ {1}".format(result.value, result.params))


def main(args):
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.DEBUG)

    if len(args) > 0 and (args[0] == 's' or args[0] == 'silent'):
        logging.disable(logging.CRITICAL)

    # for threadWorker in simple_thread_workers:
    #     testSimpleThreadWorkerEvaluation()
    testSimpleSocketWorkerEvaluation(SimplePreemptibleSocketWorker)

if __name__ == '__main__':
    main(sys.argv[1:])
