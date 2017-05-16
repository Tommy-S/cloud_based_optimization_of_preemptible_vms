from poap.controller import BasicWorkerThread, BaseWorkerThread, ProcessWorkerThread
from poap.tcpserve import SocketWorker, ProcessSocketWorker, SocketWorkerHandler
import logging
from threading import Thread
import socket
import Queue
import traceback
import sys
from collections import deque

TIMEOUT = 0
# Get module-level logger
logger = logging.getLogger(__name__)


class SimpleWorker(object):
    def __init__(self, objective):
        self.objective = objective


class ProcessWorker(object):
    def __init__(self):
        self.process = None

    def _kill_process(self):
        if self.process_is_running():
            logger.debug("ProcessWorker is killing subprocess")
            self.process.terminate()

    def handle_kill(self):
        self._kill_process()

    def handle_terminate(self):
        self._kill_process()

    def process_is_running(self):
        return self.process is not None and self.process.poll() is None


class BaseEventWorker(object):
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

    def __init__(self):
        """Initialize the EventWorker."""
        self._BaseEventWorkerInit = True
        self.messageQueue = Queue.Queue()
        self.requestQueue = deque()
        self.running = False

    def run(self, loop=True):
        """Main loop."""
        self.running = True
        self._run()
        while loop and self.running:
            self._run()

    def _run(self):
        for _ in range(self.messageQueue.qsize()):
            try:
                handler = self.messageQueue.get_nowait()
                method = handler[0]
                args = handler[1]
                method(*args)
            except Queue.Empty:
                pass

        request = self.examine_incoming_request(self.receive_request())
        if request is not None:
            self.requestQueue.append(request)

        try:
            request = self.requestQueue.popleft()
            if self.can_run_request(request):
                self.run_request(request)
            else:
                self.requestQueue.appendleft(request)
        except IndexError:
            pass

    def can_run_request(self, request):
        return True

    def message_self(self, fn, args=[]):
        self.messageQueue.put((fn, args))

    # def receive_request(self, timeout=1):
    #     return None

    # def examine_incoming_request(self, request):
    #     return request

    # def run_request(self, request):
    #     return

    def handle_eval(self, *params):
        self.evaluate(*params)


class BaseEventWorkerThread(BaseWorkerThread):
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
            logger.debug("Exit worker thread run()")
            self.running = False
            return None
        else:
            return request

    def run_request(self, request):
        if request[0] == 'eval':
            logger.debug("Worker thread received eval request")
            record = request[1]
            self.add_message(record.running)
            self.handle_eval(record)
        else:
            logger.warning("Worker received unrecognized request: {0}".format(request[0]))


class BasicEventWorkerThread(BaseEventWorker, BaseEventWorkerThread, SimpleWorker):
    def __init__(self, controller, objective):
        BaseEventWorkerThread.__init__(self, controller)
        BaseEventWorker.__init__(self)
        SimpleWorker.__init__(self, objective)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            self.finish_success(record, value)
            logger.debug("Worker finished feval successfully")
        except Exception:
            self.finish_cancelled(record)
            logger.debug("Worker feval exited with exception")


class EventProcessWorkerThread(BaseEventWorkerThread, BaseEventWorker, ProcessWorker):
    def __init__(self, controller):
        BaseEventWorkerThread.__init__(self, controller)
        BaseEventWorker.__init__(self)
        ProcessWorker.__init__(self)

    def process_is_running(self):
        return self.process is not None and self.process.poll() is None

    def can_run_request(self, request):
        return not self.process_is_running()


class BaseInterruptibleWorker(BaseEventWorker):
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

    def __init__(self):
        """Initialize the InterruptibleWorker."""
        BaseEventWorker.__init__(self)
        self.evaluating = False
        self.evalParams = None
        self.evalThread = Thread(target=lambda: None)

    def evaluate(self, *params):
        """
        Evaluate the optimization function and package the results for handling.
        Should only be called from the preemptible evaluation thread.
        _eval itself should not communicate with the controller.
        Communication with the controller is done in the function callback that
        _eval returns- this is to prevent interleaving a preemption message into
        _eval completion messages.

        Args:
            params: placeholder function signature
                    Must have the same signature as finish_preempted.

        Returns:
            A tuple or list containing a callback function and its arguments.
            return[0] is a function object
            return[1:] are the arguments to that function
        """
        return (lambda: None,)

    def handle_eval(self, *params):
        """
        Wrap optimization function evaluation so it can be canceled in case of an interruption.

        Args:
            params: passed through to _eval and finish_interrupted
        """
        self.evalParams = params
        self.evaluating = True

        def interruptible_eval(evalResults):
            evalResults[:] = self.evaluate(*params)
            if self.evaluating:
                self.message_self(evalResults[0], evalResults[1:])

        evalResults = []
        evalThread = Thread(target=interruptible_eval, args=(evalResults,))
        evalThread.daemon = True
        evalThread.start()

    def can_run_request(self, request):
        return not self.evaluating and not self.evalThread.isAlive()

    def handle_kill(self, *params):
        if self.evaluating:
            self.evaluating = False
            evalParams = self.evalParams
            self.evalParams = None
            self.finish_killed(evalParams)


class BaseInterruptibleWorkerThread(BaseInterruptibleWorker, BaseEventWorkerThread):
    def __init__(self, controller):
        """Initialize the EventWorker."""
        BaseInterruptibleWorker.__init__(self)
        BaseEventWorkerThread.__init__(self, controller)


class BasicInterruptibleWorkerThread(BaseInterruptibleWorkerThread, SimpleWorker):
    def __init__(self, controller, objective):
        BaseInterruptibleWorkerThread.__init__(self, controller)
        SimpleWorker.__init__(self, objective)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            logger.debug("Worker finished feval successfully")
            return [self.finish_success, record, value]
        except Exception:
            logger.debug("Worker feval exited with exception")
            return [self.finish_cancelled, record]


class InterruptibleEventProcessWorkerThread(BaseInterruptibleWorkerThread, ProcessWorker):
    def __init__(self, controller):
        BaseInterruptibleWorkerThread.__init__(self)
        ProcessWorker.__init__(self)

    def can_run_request(self, request):
        return not self.process_is_running() and BaseInterruptibleWorkerThread.can_run_request(self, request)
