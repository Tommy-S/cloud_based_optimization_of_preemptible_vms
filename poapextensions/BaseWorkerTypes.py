import logging
from threading import Thread
from collections import deque

# Get module-level logger
logger = logging.getLogger(__name__)


class BaseEventWorker(object):
    """
    Sets up the machinery for event-driven workers.
    This class is intended to be used in multiple inheritance
    with other POAP workers.

    This class ascribes an order to message handling.
    BaseEventWorkers maintain a queue of actions to take called the
    messageQueue. Messages on the queue take the form
    [function object, arg1, arg2, ...]. Messages are handled
    in a FIFO manner.

    BaseEventWorkers do not implement the full POAP worker interface.

    Inheriting classes need to implement:
    (1) receive_request(self) : request
        Handle requests from the controller. The format of a request
        is determined by inheriting classes.

    (2) examine_incoming_request(self, request) : request | None
        Special handling for incoming requests, if necessary.
        Passes through requests that can be executed normally.

    (3) run_request(self, request) : unit
        Process a normal request from the controller.
        This should translate requests into messages and add them
        to the messageQueue.

    (4) evaluate(self, *params) : [function object, *args]
        Evaluate the local optimization function. Return type must
        be in the form of a message. Inheriting classes overload this.

    """

    def __init__(self):
        """Initialize the EventWorker."""
        self.messageQueue = deque()
        self.requestQueue = deque()
        self.running = False

    def run(self, loop=True):
        """Main loop."""
        self.running = True
        if hasattr(self, 'setname'):  # For workers running in threads
            self.setname('ThreadWorker')
        self._run()
        while loop and self.running:
            self._run()
        logger.info("Worker exiting run()")

    def _run(self):
        for _ in range(len(self.messageQueue)):
            if not self.running:
                return
            try:
                handler = self.messageQueue.popleft()
                method = handler[0]
                args = handler[1]
                method(*args)
            except IndexError:
                pass

        if not self.running:
            return

        request = self.examine_incoming_request(self.receive_request())
        if not self.running:
            return
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

    def message_self(self, fn, args=[]):
        self.messageQueue.append((fn, args))

    def can_run_request(self, request):
        return True

    def handle_eval(self, *params):
        self.message_self(self.evaluate(*params))


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
                def finish_eval():
                    evalResults[0](*evalResults[1:])
                    self.evaluating = False
                self.message_self(finish_eval)

        evalResults = []
        evalThread = Thread(target=interruptible_eval, args=(evalResults,), name='EvalThread')
        evalThread.daemon = True
        evalThread.start()
        logger.debug("Exiting interruptible_eval")

    def can_run_request(self, request):
        return not self.evaluating and not self.evalThread.isAlive()

    def handle_kill(self, *params):
        if self.evaluating:
            self.evaluating = False
            evalParams = self.evalParams
            self.evalParams = None
            self.finish_killed(evalParams)


class BasePreemptibleWorker(BaseInterruptibleWorker):
    def __init__(self):
        BaseInterruptibleWorker.__init__(self)

    def run(self):
        def detect_preemption():
            """
            Repeatedly check for preemption events.
            If one has been found, set the preemption flag and release the
            semaphore to interrupt any ongoing optimization function evaluation.
            """
            if self.is_preempted():
                logger.debug("Worker detected preemption event")
                self.handle_preempt()
            else:
                self.message_self(detect_preemption)

        self.message_self(detect_preemption)
        BaseInterruptibleWorker.run(self)

    def _run(self):
        BaseInterruptibleWorker._run(self)

    def handle_preempt(self):
        if self.evaluating and hasattr(self, 'finish_preempted'):
            self.finish_preempted(*self.evalParams)
        self.running = False
        self.evaluating = False
        self.preempt()

    def finish_preempted(self, *params):
        pass

    def is_preempted(self):
        return False

    def preempt(self):
        logger.debug("Worker exiting from preemption")
