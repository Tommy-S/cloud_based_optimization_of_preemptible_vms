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
logging.getLogger('poap.controller').setLevel(logging.INFO)
logging.getLogger('poapextensions.StatefulPreemptionStrategy').setLevel(logging.INFO)


def findFreePort():
    port = 10000
    portopen = False
    while not portopen:
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
    return port

compute = googleapiclient.discovery.build('compute', 'v1')

# Launch all the virtual machines
numVMs = 1
vms = {}
i = 0
while len(vms) < numVMs:
    vm = GCPVMMonitor(compute, 'gcpworker0')
    if vm.start():
        vms[vm.getInternalIP()] = vm

samples = [0.0, 0.1]
strategy = RecoverableFixedSampleStrategy(samples)

hostip = socket.gethostbyname(socket.gethostname())

port = findFreePort()
name = (hostip, port)


def ncc(clientIP, socketWorkerHandler):
    vms[vm.getInternalIP()].addWorker(socketWorkerHandler)

server = RecoverableThreadedTCPServer(
    sockname=name,
    strategy=strategy,
    newConnectionCallbacks=[ncc]
)

cthread = threading.Thread(
    target=server.run,
    name='Server for {0}'.format(SimpleGCPRecoverableSocketWorker.__name__)
)
cthread.start()

# Get server IP and port
name = server.sockname
logger.debug("Launch controller at {0}".format(name))


# Launch workers
serverIP = name[0]
serverPort = name[1]
workersPerVM = 1
for _ in range(workersPerVM):
    for _, vm in vms.iteritems():
        vm.launchWorker(serverIP, serverPort)

# Wait on evaluation to conclude
cthread.join()

result = server.controller.best_point()
print("Final: {0:.3e} @ {1}".format(result.value, result.params))
# vm.stop()
