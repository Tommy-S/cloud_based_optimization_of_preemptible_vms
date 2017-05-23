from poap.tcpserve import SocketWorker as _SocketWorker
from poapextensions.BaseWorkerTypes import (
    BaseEventWorker,
    BaseInterruptibleWorker,
    BasePreemptibleWorker,
)
import logging
import socket
import traceback

# Get module-level logger
logger = logging.getLogger(__name__)


class SocketWorker(_SocketWorker):
    def __init__(self, sockname, retries=0):
        _SocketWorker.__init__(self, sockname, retries)

    def send(self, *args):
        logger.debug("Sending: {0}".format(args))
        _SocketWorker.send(self, *args)

    def receive_request(self, timeout=1):
        prevtimeout = self.sock.gettimeout()
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(4096)
            if not data:
                return None
            return self.unmarshall(data)
        except socket.timeout:
            return None
        finally:
            self.sock.settimeout(prevtimeout)

    def examine_incoming_request(self, request):
        logger.debug("Request: {0}".format(request))
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
        self.send('exit_preempted')


class RecoverableSocketWorker(PreemptibleSocketWorker):
    def __init__(self, lockClass, stateClass, sockname, retries=0):
        PreemptibleSocketWorker.__init__(self, sockname, retries)
        self.lockClass = lockClass
        self.stateClass = stateClass

    def run_request(self, request):
        if request[0] == 'recover':
            logger.debug("Worker thread received recover request")
            recoveryInfo = request[1]
            record_id = request[2]
            args = request[3:]
            self.handle_recover(recoveryInfo, record_id, *args)
        else:
            PreemptibleSocketWorker.run_request(self, request)

    def handle_eval(self, record_id, *params):
        stateLock = self.lockClass(record_id)
        state = self.stateClass(record_id)
        PreemptibleSocketWorker.handle_eval(self, record_id, *(params + (stateLock, state)))
        stateLock.cleanup()
        state.cleanup()

    def handle_recover(self, recoveryInfo, record_id, *params):
        logger.debug("Worker recovering with state: {0}".format(recoveryInfo))
        stateLock = self.lockClass(record_id)
        state = self.stateClass(record_id, recoveryInfo)
        PreemptibleSocketWorker.handle_eval(self, record_id, *(params + (stateLock, state)))
        stateLock.cleanup()
        state.cleanup()

    def finish_preempted(self, record_id, *params):
        logger.debug("Recovery Finish Preempted")
        saved = False
        stateLock = params[-2]
        state = params[-1]

        logger.debug("got statelock and state")

        try:
            if stateLock.acquire(False):
                (savedSuccessfully, savedStateInfo) = state.save()
                if savedSuccessfully:
                    saved = True
        except Exception:
            logger.error(traceback.format_exc())

        logger.debug("State is saved: {0}".format(saved))

        if saved:
            logger.debug("Preempt with state saved")
            self.finish_preempted_state_saved(savedStateInfo, record_id, params[:-2])
        else:
            logger.debug("Preempt with state unsaved")
            self.finish_preempted_state_unsaved(params[:-2])

    def finish_preempted_state_saved(self, savedStateInfo, record_id, params):
        msg = ('eval_preempted', record_id, savedStateInfo)
        self.send(*msg)
        logger.debug("Feval preempted")

    def finish_preempted_state_unsaved(self, record_id, params):
        PreemptibleSocketWorker.finish_preempted(self, record_id, params)

    def handle_kill(self, *params):
        PreemptibleSocketWorker.handle_kill(self, *params[:-2])
