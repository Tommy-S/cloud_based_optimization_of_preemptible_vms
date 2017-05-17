import sys
import time
import logging
import threading

from poap.tcpserve import SimpleSocketWorker


# Set up default host, port, and time
TIMEOUT = 0
NAME = ('0.0.0.0', 0)


def f(x):
    logging.info("Request for {0}".format(x))
    if TIMEOUT > 0:
        time.sleep(TIMEOUT)
    logging.info("OK, done")
    return (x + 1) * (x + 1)


def worker_main(name):
    logging.info("Launching worker on port {0}".format(name[1]))
    SimpleSocketWorker(f, sockname=name, retries=1).run()


def main():
    logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
                        level=logging.INFO)

    # Get controller port
    name = NAME

    # Launch workers
    wthreads = []
    for k in range(2):
        wthread = threading.Thread(target=worker_main, args=(name,))
        wthread.start()
        wthreads.append(wthread)

    # Wait on workers
    for t in wthreads:
        t.join()

    print("Threads done")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TIMEOUT = float(sys.argv[1])
        NAME = (sys.argv[2], int(sys.argv[3]))
    main()
