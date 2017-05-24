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
    """
    Base SocketWorker class for use with BaseWorkerTypes.
    Connects to a SocketWorkerHandler owned by a TCPServer.
    Implements three of the four required functions from
    BaseEventWorker.
    Combined with BaseEventWorker, SocketWorker still does
    not implement the full POAP worker interface.
    """

    def __init__(self, sockname, retries=0):
        _SocketWorker.__init__(self, sockname, retries)

    def receive_request(self, timeout=1):
        """
        Receive and parse data from the socket.
        This is one of the unimplemented functions in the
        BaseEventWorker interface.
        """
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
        """
        Filter incoming requests for time-sensitive requests.
        This is one of the unimplemented functions in the
        BaseEventWorker interface.
        Time-sensitive requests include kill and terminate
        requests, and are not subject to the can_run_request
        check before evaluation. Requests are still added to
        the messageQueue (part of BaseEventWorker) for processing.
        Requests not judged time-critical are returned.
        Time-critical requests result in None being returned.
        """
        if request is not None:
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
        """Placeholder for subclasses."""
        self.running = False

    def run_request(self, request):
        """
        Execute non-time-critical requests from the controller.
        This is one of the unimplemented functions in the
        BaseEventWorker interface.
        The assumed request format from the controller via
        the SocketWorkerHandler is (record_id, evaluationArguments)
        """
        if request[0] == 'eval':
            logger.debug("Worker thread received eval request")
            record_id = request[1]
            args = [record_id]
            args[1:] = request[2:]
            self.message_self(self.handle_eval, args)
        else:
            logger.warning("Worker received unrecognized request: {0}".format(request[0]))


class EventSocketWorker(BaseEventWorker, SocketWorker):
    """Apply the BaseEventWorker machinery to a SocketWorker."""

    def __init__(self, sockname, retries=0):
        BaseEventWorker.__init__(self)
        SocketWorker.__init__(self, sockname, retries)


class InterruptibleSocketWorker(BaseInterruptibleWorker, EventSocketWorker):
    """Apply the BaseInterruptibleWorker machinery to an EventSocketWorker."""

    def __init__(self, sockname, retries=0):
        BaseInterruptibleWorker.__init__(self)
        EventSocketWorker.__init__(self, sockname, retries)


class PreemptibleSocketWorker(BasePreemptibleWorker, EventSocketWorker):
    """Apply the BasePreemptibleWorker machinery to an EventSocketWorker."""

    def __init__(self, sockname, retries=0):
        BasePreemptibleWorker.__init__(self)
        EventSocketWorker.__init__(self, sockname, retries)

    def finish_preempted(self, record_id, params):
        """Inform the SocketWorkerHandler that evaluation was preempted."""
        msg = ('eval_preempted', record_id)
        self.send(*msg)
        logger.debug("Feval preempted")

    def preempt(self):
        """Inform the SocketWorkerHandler that the worker was preempted."""
        self.send('exit_preempted')
        self.sock.close()


class RecoverableSocketWorker(PreemptibleSocketWorker):
    """
    Add the ability to recover a different interrupted worker.
    RecoverableSocketWorkers use the Lock and State objects
    fitting the interfaces in RecoveryStates in order to
    maintain a state for checkpointing, and to modulate
    access to that state.
    """

    def __init__(self, lockClass, stateClass, sockname, retries=0):
        """
        Initialize the RecoverableSocketWorker.
        lockClass and stateClass are class objects that are
        used to create lock and state objects for optimization
        function evaluation.
        """
        PreemptibleSocketWorker.__init__(self, sockname, retries)
        self.lockClass = lockClass
        self.stateClass = stateClass

    def run_request(self, request):
        """
        Add recover to the list of accepted controller requests.
        Recover requests take the form of:
        [recoveryInfo: String, record_id, *otherArgs]
        """
        if request[0] == 'recover':
            logger.debug("Worker thread received recover request")
            recoveryInfo = request[1]
            record_id = request[2]
            args = [recoveryInfo, record_id]
            args[2:] = request[3:]
            self.message_self(self.handle_recover, args)
        else:
            PreemptibleSocketWorker.run_request(self, request)

    def handle_eval(self, record_id, *params):
        """
        Create Lock and State objects.
        Pass them in as the last two parameters to self.evaluate.
        """
        stateLock = self.lockClass(record_id)
        state = self.stateClass(record_id)
        PreemptibleSocketWorker.handle_eval(self, record_id, *(params + (stateLock, state)))
        stateLock.cleanup()
        state.cleanup()

    def handle_recover(self, recoveryInfo, record_id, *params):
        """Recover state from a previous evaluation and resume evaluation."""
        logger.debug("Worker recovering with state: {0}".format(recoveryInfo))
        stateLock = self.lockClass(record_id)
        state = self.stateClass(record_id, recoveryInfo)
        PreemptibleSocketWorker.handle_eval(self, record_id, *(params + (stateLock, state)))
        stateLock.cleanup()
        state.cleanup()

    def finish_preempted(self, record_id, *params):
        """Attempt to save state and then transmit it to the controller."""
        logger.debug("Recovery Finish Preempted")
        saved = False
        stateLock = params[-2]
        state = params[-1]

        logger.debug("got statelock and state")

        try:
            if stateLock.acquire(False):
                _savedStateInfo = state.save()
                savedSuccessfully = _savedStateInfo[0]
                if savedSuccessfully:
                    savedStateInfo = _savedStateInfo[1]
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
        """Inform the controller that evaluation was preempted."""
        msg = ('eval_preempted', record_id, savedStateInfo)
        self.send(*msg)
        logger.debug("Feval preempted")

    def finish_preempted_state_unsaved(self, record_id, params):
        PreemptibleSocketWorker.finish_preempted(self, record_id, params)

    def handle_kill(self, *params):
        """Remove lock & state from handle_kill parameters."""
        PreemptibleSocketWorker.handle_kill(self, *params[:-2])
