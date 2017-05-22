import googleapiclient.discovery
import googleapiclient.errors
import os
import time
import threading
import logging
import socket
import pickle
import errno
from poapextensions.tests import gcpworker
from poapextensions.PreemptionDetectors import GCPPreemptionDetector
from multiprocessing import Process

logger = logging.getLogger(__name__)


def launchWorker(args):
    hostIP = args[0]
    port = args[1]
    gcpworker.run(hostIP, port)


class GCPWorkerManager(GCPPreemptionDetector):
    def __init__(self, hostIP, port, retries=0, launchWorker=launchWorker):
        GCPPreemptionDetector.__init__(self)
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
        "Marshall data to wire format"
        return pickle.dumps(args)

    def unmarshall(self, data):
        "Convert data from wire format back to Python tuple"
        return pickle.loads(data)

    def send(self, *args):
        "Send a message to the controller"
        self.sock.send(self.marshall(*args))

    def _run(self):
        "Run a message from the controller"
        if not self.running:
            return
        data = self.unmarshall(self.sock.recv(4096))

        if data[0] == 'ping':
            self.ping()
        elif data[0] == 'launchWorker':
            self._launchWorker(*data[1:])

    def ping(self):
        self.send("ping")

    def _launchWorker(self, *args):
        Process(target=self.launchWorker, args=(args,)).start()

    def run(self):
        "Main loop"
        try:
            self._run()
            while self.running and not self.is_preempted():
                self._run()
        except socket.error as e:
            logger.warning("Exit loop: {0}".format(e))
        finally:
            self.sock.close()
            self.acpid_socket.close()


class GCPVMMonitor(object):
    def __init__(
        self,
        compute,
        name,
        project = 'bustling-syntax-160718',
        zone = 'us-east1-b',
        family = 'poap-debian',
        refreshTime = 10,
        preemptible = True,
        metadata = {}
    ):
        self.compute = compute
        self.name = name
        self.zone = zone
        self.family = family
        self.project = project
        self.workers = []
        self.instance = None
        self.refreshTime = refreshTime
        self.preemptible = preemptible
        self.metadata = metadata

    def is_vm_alive(self):
        self.refreshStatus()
        status = self.getStatus()
        return status in ['STAGING', 'RUNNING']

    def refreshInstance(self):
        try:
            self.instance = self.compute.instances().get(project=self.project, zone=self.zone, instance=self.name).execute()
        except googleapiclient.errors.HttpError:
            self.instance = None

    def _refreshInstance(self):
        while self.vm_alive:
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
                    msg = self.unmarshall(self.sock.recv(4096))
                    self.handleMessage(msg)
            except socket.timeout:
                pass

            time.sleep(self.refreshTime)

    def handleMessage(self, msg):
        logger.debug("Message: {0}".format(msg))

    def marshall(self, *args):
        "Marshall data to wire format"
        return pickle.dumps(args)

    def unmarshall(self, data):
        "Convert data from wire format back to Python tuple"
        return pickle.loads(data)

    def send(self, *args):
        "Send a message to the controller"
        self.sock.send(self.marshall(*args))

    def launchWorker(self, *args):
        self.send(*(['launchWorker'] + args))

    def addWorker(self, socketWorkerHandler):
        self.workers.append(socketWorkerHandler)
        logger.debug(self.name + ' Heard about a new worker')

    def getStatus(self):
        if self.instance is not None:
            return str(self.instance['status'])
        else:
            return None

    def getInternalIP(self):
        return str(self.instance['networkInterfaces'][0]['networkIP'])

    def wait_for_operation(self, operation):
        while True:
            result = self.compute.zoneOperations().get(
                project=self.project,
                zone=self.zone,
                operation=operation['name']).execute()

            if result['status'] == 'DONE':
                print("done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)

    def start(self):
        try:
            hostip = socket.gethostbyname(socket.gethostname())
            port = 10000
            portopen = False
            while not portopen:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.bind((hostip, port))
                    s.listen(5)
                    s.settimeout(0.1)
                    self.sock = s
                    portopen = True
                except socket.error as error:
                    if not error.errno == errno.EADDRINUSE:
                        raise
                    else:
                        port += 1

            self.refreshInstance()
            if self.instance is not None:
                logger.debug("Instance {0} already exists with status {1}".format(self.name, self.getStatus()))
                return False

            self.wait_for_operation(self._start(port))
            logger.debug("Startup completed")

            refreshThread = threading.Thread(
                target = self._refreshInstance,
                name = self.name + ' -- Refresh Instance'
            )
            refreshThread.daemon = True
            refreshThread.start()

            return True
        except:
            raise

    def stop(self):
        if self.instance is not None:
            self._delete_instance()

    def _start(self, port):
        # Get the latest Debian Jessie image.
        image_response = self.compute.images().getFromFamily(
            project=self.project, family=self.family).execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/%s/machineTypes/n1-standard-1" % self.zone
        startup_script = open(
            os.path.join(
                os.path.dirname(__file__), 'startup-script.sh'), 'r').read()

        config = {
            'name': self.name,
            'machineType': machine_type,
            'scheduling': {
                'preemptible': self.preemptible
            },

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],

            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [
                    {
                        # Startup script is automatically executed by the
                        # instance upon startup.
                        'key': 'startup-script',
                        'value': startup_script
                    },
                    {
                        'key': 'hostip',
                        'value': socket.gethostbyname(socket.gethostname())
                    },
                    {
                        'key': 'port',
                        'value': str(port)
                    }
                ]
            }
        }

        for key, value in self.metadata.iteritems():
            config['metadata']['items'].append({'key': key, 'value': value})

        return self.compute.instances().insert(
            project=self.project,
            zone=self.zone,
            body=config).execute()

    def _delete_instance(self):
        return self.compute.instances().delete(
            project=self.project,
            zone=self.zone,
            instance=self.name).execute()
