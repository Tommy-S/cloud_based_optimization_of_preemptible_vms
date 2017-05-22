import threading
from poapextensions.SimpleWorkers import SimpleGCPRecoverableSocketWorker
from poapextensions.StatefulPreemptionStrategy import RecoverableFixedSampleStrategy
from poapextensions.preemptibleControllers import RecoverableThreadedTCPServer
import socket
import logging
import errno
import googleapiclient.discovery
from poapextensions.GCPVirtualMachine import GCPVMMonitor

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)
logging.getLogger('poap.tcpserve').setLevel(logging.INFO)
logging.getLogger('poap.strategy').setLevel(logging.INFO)
logging.getLogger('poapextensions.StatefulPreemptionStrategy').setLevel(logging.INFO)

compute = googleapiclient.discovery.build('compute', 'v1')
vm = GCPVMMonitor(compute, 'gcpworker0')
vm.start()


def ncc(clientIP, socketWorker):
    logger.info("Worker discovered: {0}. VM IP: {1}".format(clientIP, vm.getInternalIP()))
    if clientIP == vm.getInternalIP():
        logger.info("Adding socket worker to client")
        vm.addWorker(socketWorker)
    else:
        logger.info("Not adding socket worker to client")

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
server = RecoverableThreadedTCPServer(sockname=name, strategy=strategy, newConnectionCallbacks=[ncc])
cthread = threading.Thread(target=server.run, name='Server for {0}'.format(SimpleGCPRecoverableSocketWorker.__name__))
cthread.daemon = True
cthread.start()

# Get controller port
name = server.sockname
logger.debug("Launch controller at {0}".format(name))


# Launch workers
vm.launchWorker(name[0], name[1])

# Wait on controller and workers
cthread.join()

result = server.controller.best_point()
print("Final: {0:.3e} @ {1}".format(result.value, result.params))
vm.stop()
