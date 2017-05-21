from poapextensions.SimpleWorkers import (
    SimpleRecoverableSocketWorker,
)
from poapextensions.RecoveryStates import (
    BasicLock,
    BasicStateObject,
)
from poapextensions.preemptibleControllers import (
    RecoverableThreadedTCPServer,
)
from poapextensions.StatefulPreemptionStrategy import (
    RecoverableFixedSampleStrategy,
)
import threading
import logging
import sys
"""
Test fixed sampling strategy.
"""

import socket
import errno
import time

logger = logging.getLogger(__name__)


def objective(x, stateLock, state):
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


class PreemptionTestMixIn(object):
    """
    Add a controllable preemption system to a PreemptibleBasicWorkerThread.
    This is only for unit-testing purposes.
    """

    def __init__(self, threadID, workerClass):
        self.id = threadID
        self.workerClass = workerClass

    def is_preempted(self):
        return self.id == 0

    def evaluate(self, *params):
        if self.id == 1:
            self.id = 0
        return self.workerClass.evaluate(self, *params)


class TestSimpleRecoverableSocketWorker(PreemptionTestMixIn, SimpleRecoverableSocketWorker):
    def __init__(self, threadID, objective, sockname, retries=0):
        PreemptionTestMixIn.__init__(self, threadID, SimpleRecoverableSocketWorker)
        SimpleRecoverableSocketWorker.__init__(self, objective, BasicLock, BasicStateObject, sockname, retries)

simple_socket_workers = (
    TestSimpleRecoverableSocketWorker,
)


def testSimpleSocketWorkerPreemption(socketWorker):
    """Testing routine."""
    print('<<<<<<<<<<<<<<<<<<<<  Testing Preemption on {0}  >>>>>>>>>>>>>>>>>>>'.format(socketWorker.__name__))
    # Launch controller
    samples = [0.0, 0.1]
    strategy = RecoverableFixedSampleStrategy(samples)
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
    server = RecoverableThreadedTCPServer(sockname=name, strategy=strategy)
    cthread = threading.Thread(target=server.run, name='Server for {0}'.format(socketWorker.__name__))
    cthread.start()

    # Get controller port
    name = server.sockname
    logging.debug("Launch controller at {0}".format(name))

    # Launch workers
    def worker_main(name, threadID):
        logging.debug("Launching worker on port {0}".format(name[1]))
        socketWorker(threadID, objective, name, 1).run()

    wthreads = []
    for threadID in range(4):
        """
        Worker 0 will be pre-empted as soon as it is launched.
        Worker 1 will be pre-empted as soon as an eval is launched.
        Workers 2 and 3 will execute the requests.
        """
        wthread = threading.Thread(target=worker_main, args=(name, threadID), name='SocketWorker')
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

    for socketWorker in simple_socket_workers:
        testSimpleSocketWorkerPreemption(socketWorker)

if __name__ == '__main__':
    main(sys.argv[1:])
