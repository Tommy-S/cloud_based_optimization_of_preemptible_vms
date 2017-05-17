from poap.controller import BaseWorkerThread
from surrogateGCP.poapextensions.BaseWorkerTypes import (
    BaseEventWorker,
    BaseInterruptibleWorker,
    BasePreemptibleWorker,
)
import logging
import Queue

# Get module-level logger
logger = logging.getLogger(__name__)


class ThreadWorker(BaseWorkerThread):
    """
    Sets up the machinery for pre-emptible workers.
    This class is intended to be used in multiple inheritance
    with other POAP workers.

    Preemptible workers use one thread to detect preemption events and
    another for evaluation. During optimization function, the root worker
    thread blocks until either a preemption event is detected or
    optimization function evaluation completes.

    Inheriting classes need to implement:
    (1) is_preempted(self) : bool
    (2) _eval(self, params) : unit
    (3) finish_preempted(self, params) : unit

    """

    def __init__(self, controller):
        BaseWorkerThread.__init__(self, controller)

    def receive_request(self, timeout=1):
        try:
            request = self.queue.get(True, timeout)
            return request
        except Queue.Empty:
            return None

    def examine_incoming_request(self, request):
        if request is None:
            return None
        elif request[0] == 'kill':
            logger.debug("Worker thread received kill request")
            self.message_self(self.handle_kill, request[1])
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
            record = request[1]
            self.add_message(record.running)
            self.handle_eval(record)
        else:
            logger.warning("Worker received unrecognized request: {0}".format(request[0]))


class EventThreadWorker(BaseEventWorker, ThreadWorker):
    def __init__(self, controller):
        BaseEventWorker.__init__(self)
        ThreadWorker.__init__(self, controller)

    def can_run_request(self, request):
        return True


class InterruptibleThreadWorker(BaseInterruptibleWorker, ThreadWorker):
    def __init__(self, controller):
        """Initialize the EventWorker."""
        BaseInterruptibleWorker.__init__(self)
        ThreadWorker.__init__(self, controller)


class PreemptibleThreadWorker(BasePreemptibleWorker, ThreadWorker):
    def __init__(self, controller):
        """Initialize the EventWorker."""
        BasePreemptibleWorker.__init__(self)
        ThreadWorker.__init__(self, controller)

    def finish_preempted(self, record):
        logger.debug("Feval preempted")
        self.add_message(record.cancel)
