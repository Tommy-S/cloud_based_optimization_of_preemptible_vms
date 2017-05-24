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

    BaseEventWorkers do not implement the full POAP worker interface:
    they are designed to be used in multiple-inheritance with
    other worker structures that implement the remainder of the worker
    interface, such as SocketWorkers or ThreadWorkers.

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
        self.messageQueue = deque()  # Holds executable messages
        self.requestQueue = deque()  # Holds requests from the controller
        self.running = False

    def run(self, loop=True):
        """Main loop."""
        self.running = True
        self._run()
        while loop and self.running:
            self._run()
        logger.info("Worker exiting run()")

    def _run(self):
        """
        One iteration of the main loop.
        First, process everything in the executable message queue
        at the start of this iteration. This prevents getting stuck
        in infinite message loops without processing controller
        requests.
        Then process a request from the controller, if one is available.
        """
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
        """
        Add a function callback and arguments to the message queue.
        All additions to the message queue should go through here.
        """
        self.messageQueue.append((fn, args))

    def can_run_request(self, request):
        """
        Placeholder for later subclasses.
        Decide if conditions are suitable to execute a
        request from the controller.
        """
        return True

    def handle_eval(self, *params):
        """
        Wrapper for self.evaluate(*params).
        Subclasses may override this function to provide
        different structure for optimization function evaluation.

        @parameter params should be passed through to self.evaluate().
        """
        self.message_self(self.evaluate(*params))


class BaseInterruptibleWorker(BaseEventWorker):
    """
    Sets up the machinery for interruptible workers.

    Interruptible workers run the evaluation function in a
    thread so that the main worker thread can continue to
    process requests from the controller and other messages
    such as kill and terminate requests.

    Inheriting classes need to implement the same functions
    as with the BaseEventWorker class.
    """

    def __init__(self):
        """Initialize the InterruptibleWorker."""
        BaseEventWorker.__init__(self)
        self.evaluating = False
        self.evalParams = None
        self.evalThread = Thread(target=lambda: None)

    def handle_eval(self, *params):
        """
        Run evaluation in a thread so interruptions can occur.

        Args:
            params: passed through to evaluate() and finish_interrupted()
        """
        self.evalParams = params
        self.evaluating = True

        def interruptible_eval():
            evalResults = self.evaluate(*params)

            def finish_eval():
                evalResults[0](*evalResults[1:])
                self.evaluating = False
            if self.evaluating:
                self.message_self(finish_eval)

        evalThread = Thread(target=interruptible_eval, name='EvalThread')
        evalThread.daemon = True
        evalThread.start()
        logger.debug("Exiting interruptible_eval")

    def can_run_request(self, request):
        """True iff an existing evaluation is not already running."""
        return not self.evaluating and not self.evalThread.isAlive()

    def handle_kill(self, *params):
        """
        Mark evaluation complete even iff it is currently running.
        Python does not support killing threads, but that would
        happen here if possible.
        """
        if self.evaluating:
            self.evaluating = False
            evalParams = self.evalParams
            self.evalParams = None
            self.finish_killed(evalParams)


class BasePreemptibleWorker(BaseInterruptibleWorker):
    """
    Add preemption as a type of evaluation interruption.

    Inheriting classes need to implement the same functions
    as with the BaseEventWorker class, as well as:

    (1) finish_preempted(self, *params) : unit
        Handle cleanup of pre-empted evaluation

    (2) is_preempted(self) : boolean
        Detect preemption events
    """

    def __init__(self):
        """Initialize the superclass."""
        BaseInterruptibleWorker.__init__(self)

    def run(self):
        """Initialize preemption detection, then start the main loop."""
        def detect_preemption():
            """
            Repeatedly check for preemption events.
            If one has been found, handle it. Otherwise add this
            function back to the messageQueue to check again.
            """
            if self.is_preempted():
                logger.debug("Worker detected preemption event")
                self.handle_preempt()
            else:
                self.message_self(detect_preemption)

        self.message_self(detect_preemption)
        BaseInterruptibleWorker.run(self)

    def handle_preempt(self):
        """Clean up and shut down the worker after a preemption event."""
        if self.evaluating:
            self.finish_preempted(*self.evalParams)
        self.running = False
        self.evaluating = False
        self.preempt()

    def is_preempted(self):
        """Detect preemption events. Placeholder."""
        return False

    def preempt(self):
        """Handle worker-specific shutdown procedures."""
        logger.debug("Worker exiting from preemption")
