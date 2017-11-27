import os
import time
import threading
import logging
import socket
import pickle
import errno
from poapextensions import SampleGCEWorker
from poapextensions.PreemptionDetectors import GCEPreemptionDetector
from multiprocessing import Process
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def launchWorker(args):
    """Launch a SocketWorker on a given host and port."""
    hostIP = args[0]
    port = args[1]
    SampleGCEWorker.run(hostIP, port)


class GCEWorkerManager(GCEPreemptionDetector):
    """
    Communicate with a GCEVMMonitor.
    Initialize and run this class on a Google Compute Engine Debian server.
    Intended to be run from the startup script.
    """

    def __init__(self, hostIP, port, retries=0, launchWorker=launchWorker):
        """
        Initialize GCEWorkerManager.
        launchWorker is a function object that takes String arguments
        from the GCEVMMonitor to launch SocketWorkers.
        """
        GCEPreemptionDetector.__init__(self)
        self.hostIP = hostIP
        self.port = port
        self.launchWorker = launchWorker
        self.running = False
        while not self.running and retries >= 0:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((hostIP, port))
                self.running = True
            except socket.error as e:
                logger.warning("Worker could not connect: {0}".format(e))
                retries -= 1
                time.sleep(1)

    def marshall(self, *args):
        """Marshall data to wire format."""
        return pickle.dumps(args)

    def unmarshall(self, data):
        """Convert data from wire format back to Python tuple."""
        return pickle.loads(data)

    def send(self, *args):
        """Send a message to the controller."""
        self.sock.send(self.marshall(*args))

    def _run(self):
        """Run a message from the controller."""
        if not self.running:
            return
        data = self.sock.recv(4096)
        if not data:
            return
        msg = self.unmarshall(data)

        if msg[0] == 'ping':
            self.ping()
        elif msg[0] == 'launchWorker':
            self._launchWorker(*msg[1:])

    def ping(self):
        """Heartbeat."""
        self.send("ping")

    def _launchWorker(self, *args):
        """
        Launch workers in a separate process.
        On preemption, they are on their own.
        This class is only responsible for passing arguments
        through to self.launchWorker.
        """
        Process(target=self.launchWorker, args=(args,)).start()

    def run(self):
        """Main loop."""
        try:
            self._run()
            while self.running and not self.is_preempted():
                self._run()
        except socket.error as e:
            logger.warning("Exit loop: {0}".format(e))
        finally:
            self.sock.close()
            self.acpid_socket.close()


class AWSVMMonitor(object):
    """
    Wrap creation and deletion of Google Compute Engine servers for use with POAP.
    For example usage see poapextensions.tests/test_gce_execution
    """

    def __init__(
        self,
        compute,
        name,
        refreshTime = 10,
        preemptible = True,
    ):
        self.compute = compute
        self.name = name
        self.workers = []
        self.instance = None
        self.refreshTime = refreshTime
        self.preemptible = preemptible
#TODO
    def is_vm_alive(self):
        '''
        status = self.getStatus()
        return status in ['STAGING', 'RUNNING']
        '''
        status = self.getStatus()
        return status in ['pending', 'running']
#TODO
    '''
    def refreshInstance(self):
        try:
            #self.instance = self.compute.instances().get(project=self.project, zone=self.zone, instance=self.name).execute()
            

        #except googleapiclient.errors.HttpError:
        except ClientError as e:
            self.instance = None
    '''

    '''
    def _refreshInstance(self):
        while self.is_vm_alive:
            self.refreshInstance()
            deadWorkers = []
            for worker in self.workers:
                if not worker.is_alive():
                    deadWorkers.append(worker)
                else:
                    pass
                    # TODO: worker.ping()
            for deadWorker in deadWorkers:
                try:
                    self.workers.remove(deadWorker)
                except ValueError:
                    pass

            try:
                while True:
                    data = self.sock.recv(4096)
                    if not data:
                        break
                    msg = self.unmarshall(data)
                    self.handleMessage(msg)
            except socket.timeout:
                pass

            time.sleep(self.refreshTime)
    '''

    def handleMessage(self, msg):
        logger.debug("Message: {0}".format(msg))

    def marshall(self, *args):
        """Marshall data to wire format."""
        return pickle.dumps(args)

    def unmarshall(self, data):
        """Convert data from wire format back to Python tuple."""
        return pickle.loads(data)

    def send(self, *args):
        """Send a message to the controller."""
        self.sock.send(self.marshall(*args))

    def launchWorker(self, *args):
        self.send(*(('launchWorker',) + args))

    def addWorker(self, socketWorkerHandler):
        self.workers.append(socketWorkerHandler)
        logger.debug(self.name + ' Heard about a new worker')
#TODO
    def getStatus(self):
        if self.instance is not None:
            status = str(self.instance['Instances'][0]['State']['Name'])
            return status
        else:
            return None
#TODO
    def getInternalIP(self):
        return str(self.instance['Instances'][0]['NetworkInterfaces'][0]['PrivateIpAddress'])
#TODO
    '''
    def wait_for_operation(self, operation):
        while True:
            result = self.compute.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=operation['name']).execute()

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)
    '''
#TODO
    def start(self):
        try:
            hostip = socket.gethostbyname(socket.gethostname())
            port = 44100
            portopen = False
            while not portopen:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.bind(('', 44100))
                    s.listen(1000)
                    portopen = True
                except socket.error as error:
                    if not error.errno == errno.EADDRINUSE:
                        raise
                    else:
                        port += 1
            
            self.instance = self.compute.run_instances(
                                ImageId='ami-8c1be5f6',
                                InstanceType='t2.micro',
                                KeyName='poaptest',
                                MaxCount=1,
                                MinCount=1,
                                UserData= """
                                                #!/bin/bash

                                                sudo yum upgrade -y
                                                sudo yum install -y git
                                                pip install boto3

                                                mkdir /playground
                                                cd /playground
                                                git clone https://github.com/dbindel/POAP.git
                                                git clone https://github.com/Tommy-S/cloud_based_optimization_of_preemptible_vms.git

                                                cp -r POAP/poap cloud_based_optimization_of_preemptible_vms 
                                                cd cloud_based_optimization_of_preemptible_vms

                                                touch runfile.py
                                                echo 'import logging' >> runfile.py
                                                echo 'logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)' >> runfile.py

                                                echo 'import sys' >> runfile.py
                                                echo 'ip = "54.205.26.158" ' >> runfile.py
                                                echo 'from poapextensions.AWSVirtualMachine import GCEWorkerManager' >> runfile.py
                                                echo 'GCEWorkerManager(ip, 44100, retries=1).run()' >> runfile.py

                                                python runfile.py 
                                        """
                            )            
            '''
            if self.instance is not None:
                logger.debug("Instance {0} already exists with status {1}".format(self.name, self.getStatus()))
                return False
            '''

            #self.wait_for_operation(self._start(port))
            #self.refreshInstance()
            #logger.debug("VM {0} running on {1}".format(self.name, self.getInternalIP()))
        

            conn, addr = s.accept()
            s.settimeout(0.1)
            self.sock = conn
            logger.info("Finished initializing VM {0} running on {1}".format(self.name, self.getInternalIP()))
            '''
            refreshThread = threading.Thread(
                target = self._refreshInstance,
                name = self.name + ' -- Refresh Instance'
            )
            refreshThread.daemon = True
            refreshThread.start()
            '''

            return True
        except:
            raise
    
    def stop(self):
        if self.instance is not None:
            self.sock.close()
            self.compute.terminate_instances(InstanceIds=[self.instance['Instances'][0]['InstanceId']])
    

