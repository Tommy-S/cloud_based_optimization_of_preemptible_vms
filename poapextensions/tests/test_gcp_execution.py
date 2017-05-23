import threading
from poapextensions.SimpleWorkers import SimpleGCPRecoverableSocketWorker
from poapextensions.StatefulPreemptionStrategy import RecoverableFixedSampleStrategy
from poapextensions.preemptibleControllers import RecoverableThreadedTCPServer
import socket
import logging
import errno
import googleapiclient.discovery
from poapextensions.GCPVirtualMachine import GCPVMMonitor
import Queue


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)
logging.getLogger('poap.tcpserve').setLevel(logging.INFO)
logging.getLogger('poap.strategy').setLevel(logging.INFO)
logging.getLogger('poap.controller').setLevel(logging.INFO)
logging.getLogger('poapextensions.StatefulPreemptionStrategy').setLevel(logging.INFO)
logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)


def findFreePort(startPort=10000):
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


def testVMExecution(numVMs, workersPerVM, project)
    compute = googleapiclient.discovery.build('compute', 'v1')

    # Launch all the virtual machines
    vms = {}
    i = 0
    launchThreads = Queue.Queue()


    def launchVM(vmName):
        vm = GCPVMMonitor(compute, vmName, project)
        if vm.start():
            vms[vm.getInternalIP()] = vm

    while len(vms) < numVMs:
        if launchThreads.qsize() + len(vms) < numVMs:
            vmName = 'gcpworker' + str(i)
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

    port = findFreePort()
    name = (hostip, port)

    server = RecoverableThreadedTCPServer(
        sockname=name,
        strategy=strategy,
        newConnectionCallbacks=[
            # Pair a new socketWorkerHandler with the VM on which it is running
            lambda clientIP, socketWorkerHandler: vms[clientIP].addWorker(socketWorkerHandler)
        ]
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
    project = 'bustling-syntax-160718'
    testVMExecution(numVMs, workersPerVM, project)

if __name__ == '__main__':
    main(sys.argv[1:])
