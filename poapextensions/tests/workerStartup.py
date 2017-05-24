import sys
import time
import logging

from poapextensions.SimpleWorkers import (
    SimpleGCEPreemptibleSocketWorker,
)


# Set up default host, port, and time
TIMEOUT = 0
NAME = ('0.0.0.0', 0)

# Get module-level logger
logger = logging.getLogger(__name__)


def f(x):
    logging.info("Request for {0}".format(x))
    if TIMEOUT > 0:
        time.sleep(TIMEOUT)
    logging.info("OK, done")
    return (x + 1) * (x + 1)


class TestSimpleGCEPreemptibleSocketWorker(SimpleGCEPreemptibleSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleGCEPreemptibleSocketWorker.__init__(self, f, sockname, retries)

    def evaluate(self, record_id, params):
        raw_input("Hit enter to start evaluation")
        return SimpleGCEPreemptibleSocketWorker.evaluate(self, record_id, params)


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.INFO)

    # Get controller port
    name = NAME

    # Launch worker
    print(NAME)
    TestSimpleGCEPreemptibleSocketWorker(f, name, 1).run()

    print("Worker done")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        NAME = (sys.argv[1], int(sys.argv[2]))
    main()
