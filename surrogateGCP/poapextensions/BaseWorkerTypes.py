import logging
from threading import Thread
from collections import deque

# Get module-level logger
logger = logging.getLogger(__name__)


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
        self.messageQueue = deque()
        self.requestQueue = deque()
        self.running = False

    def run(self, loop=True):
        """Main loop."""
        self.running = True
        if hasattr(self, 'setname'):
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
                logger.debug("IndexError")
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

    # def receive_request(self, timeout=1):
    #     return None

    # def examine_incoming_request(self, request):
    #     return request

    # def run_request(self, request):
    #     return

    def handle_eval(self, *params):
        self.evaluate(*params)


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

    def finish_preempted(self, params):
        pass

    def is_preempted(self):
        return False

    def preempt(self):
        logger.debug("Worker exiting from preemption")
