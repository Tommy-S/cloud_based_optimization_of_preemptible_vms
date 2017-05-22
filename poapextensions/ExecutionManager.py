import threading
from poapextensions.SimpleWorkers import SimpleGCPRecoverableSocketWorker
from poapextensions.StatefulPreemptionStrategy import RecoverableFixedSampleStrategy
from poapextensions.preemptibleControllers import RecoverableThreadedTCPServer
from poapextensions import create_instance
import socket
import logging
import errno
import googleapiclient.discovery

logger = logging.getLogger(__name__)

numWorkers = 2
project = 'bustling-syntax-160718'
zone = 'us-east1-b'
bucket = 'demobucket_paulwest4'
family = 'poap-debian'


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
cthread = threading.Thread(target=server.run, name='Server for {0}'.format(SimpleGCPRecoverableSocketWorker.__name__))
cthread.start()

# Get controller port
name = server.sockname
logging.debug("Launch controller at {0}".format(name))


# Launch workers
compute = googleapiclient.discovery.build('compute', 'v1')

metadata = {"hostip": hostip, 'port': str(port)}

for workerNum in range(numWorkers):
    instance_name = 'gcpworker' + str(workerNum)
    operation = create_instance.create_instance(compute, project, zone, instance_name, bucket, family=family, metadata=metadata)

    def wfc(compute, project, zone, operation, workerNum):
        create_instance.wait_for_operation(compute, project, zone, operation['name'])
        logger.info("Worker {0} has been created".format(workerNum))
    waitForComplete = threading.thread(target = wfc, args=(compute, project, zone, operation, workerNum))
    waitForComplete.start()

# Wait on controller and workers
cthread.join()
