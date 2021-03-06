import threading
from poapextensions.SimpleWorkers import SimpleGCERecoverableSocketWorker
from poapextensions.RecoverableStrategies import RecoverableFixedSampleStrategy
from poapextensions.PreemptibleControllers import RecoverableThreadedTCPServer
from poapextensions.AWSVirtualMachine import AWSVMMonitor
import socket
import logging
import errno
import Queue
import sys
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)
logging.getLogger('poap.tcpserve').setLevel(logging.INFO)
logging.getLogger('poap.strategy').setLevel(logging.INFO)
logging.getLogger('poap.controller').setLevel(logging.INFO)
logging.getLogger('poapextensions.RecoverableStrategies').setLevel(logging.INFO)



def findFreePort(hostip, startPort=10000):
    port = startPort
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
    


def testVMExecution(numVMs, workersPerVM):
    ec2 = boto3.client('ec2')

    # Launch all the virtual machines
    vms = {}
    i = 0
    launchThreads = Queue.Queue()

    def launchVM(vmName):
        vm = AWSVMMonitor(ec2, vmName)
        if vm.start():
            vms[vm.getInternalIP()] = vm

    while len(vms) < numVMs:
        if launchThreads.qsize() + len(vms) < numVMs:
            vmName = 'gceworker' + str(i)
            i += 1
            vmLaunch = threading.Thread(target = launchVM, args=(vmName,))
            vmLaunch.start()
            launchThreads.put(vmLaunch)
        else:
            launchThreads.get().join()

    logger.info("All VMs are launched and initialized")

    samples = [0.0, 0.1]
    strategy = RecoverableFixedSampleStrategy(samples)

    hostip = socket.gethostbyname(socket.gethostname())
    port = findFreePort(hostip)

    name = (hostip, port)

    server = RecoverableThreadedTCPServer(
        sockname=name,
        strategy=strategy,
        newConnectionCallbacks=[
            # Pair a new socketWorkerHandler with the VM on which it is running
            lambda clientAddr, socketWorkerHandler: vms[clientAddr[0]].addWorker(socketWorkerHandler)
        ]
    )

    cthread = threading.Thread(
        target=server.run,
        name='Server for {0}'.format(SimpleGCERecoverableSocketWorker.__name__)
    )
    cthread.start()

    # Get server IP and port
    name = server.sockname
    logger.debug("Launch controller at {0}".format(name))

    # Launch workers
    serverIP = name[0]
    serverPort = name[1]
    for _ in range(workersPerVM):
        for _, vm in vms.iteritems():
            vm.launchWorker(serverIP, serverPort)

    # Wait on evaluation to conclude
    cthread.join()

    result = server.controller.best_point()
    print("Final: {0:.3e} @ {1}".format(result.value, result.params))

    raw_input("Press Enter to shut down all worker VMs")
    for _, vm in vms.iteritems():
        try:
            vm.stop()
        except:
            logger.error("Could not stop VM {0} at {0}".format(vm.name, vm.getInternalIP()))


def main(args):
    numVMs = 1
    workersPerVM = 1
    testVMExecution(numVMs, workersPerVM)

if __name__ == '__main__':
    main(sys.argv[1:])
