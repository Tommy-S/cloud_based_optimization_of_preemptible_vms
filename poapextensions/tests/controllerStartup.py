import sys
import logging
import socket
import errno

from poapextensions.SimpleWorkers import (
    SimpleGCPRecoverableSocketWorker,
)
from poapextensions.preemptibleControllers import RecoverableThreadedTCPServer
from poapextensions.StatefulPreemptionStrategy import RecoverableFixedSampleStrategy

# Set up default host, port, and time
TIMEOUT = 0

# Get module-level logger
logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.INFO)

    """Testing routine."""
    socketWorker = SimpleGCPRecoverableSocketWorker
    print('<<<<<<<<<<<<<<<<<<<<  Testing Evaluation on {0}  >>>>>>>>>>>>>>>>>>>'.format(socketWorker.__name__))
    # Launch controller
    samples = [0.0, 0.1]
    strategy = RecoverableFixedSampleStrategy(samples)
    hostip = socket.gethostbyname(socket.gethostname())

    port = 5000
    portopen = False
    while not portopen and port < 6000:
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
    logging.info("Launch controller at {0}".format(name))

    server = RecoverableThreadedTCPServer(sockname=name, strategy=strategy)
    server.run()

    result = server.controller.best_point()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TIMEOUT = float(sys.argv[1])
    main()
