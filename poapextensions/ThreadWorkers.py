from poap.controller import BaseWorkerThread
from poapextensions.BaseWorkerTypes import (
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
    Sets up the machinery for workers to operate in a thread.

    Assumes connection with a ThreadController.
    """

    def __init__(self, controller):
        BaseWorkerThread.__init__(self, controller)

    def receive_request(self, timeout=1):
        """
        Adapt the poap.controller.BaseWorkerThread interface to an EventWorker.
        Does not implement the complete POAP worker interface.
        """
        try:
            request = self.queue.get(True, timeout)
            return request
        except Queue.Empty:
            return None

    def examine_incoming_request(self, request):
        """
        Special handling for incoming requests, if necessary.
        Passes through requests that can be executed normally.
        Implements one of the required functions in the BaseEventWorker
        interface.
        """
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
        """
        Placeholder.
        Implements one of the required functions in the BaseEventWorker
        interface.
        """
        self.running = False

    def run_request(self, request):
        if request[0] == 'eval':
            logger.debug("Worker thread received eval request")
            record = request[1]
            self.add_message(record.running)
            self.message_self(self.handle_eval, [record])
        else:
            logger.warning("Worker received unrecognized request: {0}".format(request[0]))


"""
The following classes all combine BaseEventWorker types with
ThreadWorker to create ThreadWorkers that are close to
implementing the POAP worker interface.
"""


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
