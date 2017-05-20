import sys
import logging
import threading
import socket
import errno
import urllib2

from poap.strategy import FixedSampleStrategy

from surrogateGCP.poapextensions.SimpleWorkers import (
    SimpleGCPPreemptibleSocketWorker,
)
from surrogateGCP.poapextensions.preemptibleControllers import PreemptibleThreadedTCPServer

# Set up default host, port, and time
TIMEOUT = 0

# Get module-level logger
logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.INFO)

    """Testing routine."""
    socketWorker = SimpleGCPPreemptibleSocketWorker
    print('<<<<<<<<<<<<<<<<<<<<  Testing Evaluation on {0}  >>>>>>>>>>>>>>>>>>>'.format(socketWorker.__name__))
    # Launch controller
    samples = [0.0, 0.1]
    strategy = FixedSampleStrategy(samples)
    hostip = socket.gethostbyname(socket.gethostname())
    # publicip = urllib2.urlopen('http://ip.42.pl/raw').read()

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
    logging.info("Launch controller at {0}".format(name))

    # Wait on controller
    cthread.join()

    raw_input("Hit enter to complete")

    result = server.controller.best_point()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TIMEOUT = float(sys.argv[1])
    main()
