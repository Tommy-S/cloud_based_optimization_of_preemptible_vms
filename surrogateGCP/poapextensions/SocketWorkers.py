from poap.tcpserve import SocketWorker as _SocketWorker
from surrogateGCP.poapextensions.BaseWorkerTypes import (
    BaseEventWorker,
    BaseInterruptibleWorker,
    BasePreemptibleWorker,
    BaseGCPPreemptibleWorker,
)
import logging
import socket

# Get module-level logger
logger = logging.getLogger(__name__)


class SocketWorker(_SocketWorker):
    def __init__(self, sockname, retries=0):
        _SocketWorker.__init__(self, sockname, retries)

    def receive_request(self, timeout=1):
        prevtimeout = self.sock.gettimeout()
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(4096)
            return self.unmarshall(data)
        except socket.timeout:
            return None
        finally:
            self.sock.settimeout(prevtimeout)

    def examine_incoming_request(self, request):
        if request is None:
            return None
        elif request[0] == 'kill':
            logger.debug("Worker thread received kill request")
            self.message_self(self.handle_kill, *request[1:])
            return None
        elif request[0] == 'terminate':
            logger.debug("Worker thread received terminate request")
            self.message_self(self.handle_terminate)
            return None
        else:
            return request

    def handle_terminate(self):
        self.running = False

    def run_request(self, request):
        if request[0] == 'eval':
            logger.debug("Worker thread received eval request")
            record_id = request[1]
            args = request[2:]
            self.handle_eval(record_id, *args)
        else:
            logger.warning("Worker received unrecognized request: {0}".format(request[0]))


class EventSocketWorker(BaseEventWorker, SocketWorker):
    def __init__(self, sockname, retries=0):
        BaseEventWorker.__init__(self)
        SocketWorker.__init__(self, sockname, retries)

    def can_run_request(self, request):
        return True


class InterruptibleSocketWorker(BaseInterruptibleWorker, EventSocketWorker):
    def __init__(self, sockname, retries=0):
        """Initialize the EventWorker."""
        BaseInterruptibleWorker.__init__(self)
        EventSocketWorker.__init__(self, sockname, retries)


class PreemptibleSocketWorker(BasePreemptibleWorker, EventSocketWorker):
    def __init__(self, sockname, retries=0):
        BasePreemptibleWorker.__init__(self)
        EventSocketWorker.__init__(self, sockname, retries)

    def finish_preempted(self, record_id, params):
        msg = ('eval_preempted', record_id)
        self.send(*msg)
        logger.debug("Feval preempted")

    def preempt(self):
        msg = ('exit_preempted',)
        self.send(*msg)


class GCPPreemptibleSocketWorker(BaseGCPPreemptibleWorker, EventSocketWorker):
    def __init__(self, sockname, retries=0):
        BaseGCPPreemptibleWorker.__init__(self)
        EventSocketWorker.__init__(self, sockname, retries)

    def finish_preempted(self, record_id, params):
        msg = ('eval_preempted', record_id)
        self.send(*msg)
        logger.debug("Feval preempted")

    def preempt(self):
        msg = ('exit_preempted',)
        self.send(*msg)
