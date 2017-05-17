import sys
import logging
import threading

from poap.strategy import FixedSampleStrategy
from poap.tcpserve import ThreadedTCPServer


# Set up default host, port, and time
TIMEOUT = 0


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.INFO)

    # Launch controller
    strategy = FixedSampleStrategy([1, 2, 3, 4, 5])
    server = ThreadedTCPServer(strategy=strategy)
    cthread = threading.Thread(target=server.run)
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
