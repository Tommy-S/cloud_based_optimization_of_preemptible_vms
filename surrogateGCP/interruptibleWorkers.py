from poap.controller import BaseWorkerThread as _BaseWorkerThread
from poap.tcpserve import SocketWorker
import logging
from threading import Thread
import Queue
from collections import deque
import select
import traceback

# Get module-level logger
logger = logging.getLogger(__name__)


class SimpleWorker(object):
    def __init__(self, objective):
        self.objective = objective


class ProcessWorker(object):
    def __init__(self, workerType):
        self.workerType = workerType
        self.process = None

    def _kill_process(self):
        if self.process_is_running():
            logger.debug("ProcessWorker is killing subprocess")
            self.process.terminate()

    def handle_kill(self, *args):
        self._kill_process()
        self.workerType.handle_kill(self, *args)

    def handle_terminate(self, *args):
        self._kill_process()
        self.workerType.handle_terminate(self, *args)

    def finish_preempted(self, *args):
        self._kill_process()
        self.workerType.finish_preempted(self, *args)

    def process_is_running(self):
        return self.process is not None and self.process.poll() is None

    def can_run_request(self, request):
        return not self.process_is_running() and self.workerType.can_run_request(self, request)


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


class BaseWorkerThread(_BaseWorkerThread):
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
        _BaseWorkerThread.__init__(self, controller)

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


class BaseEventWorkerThread(BaseEventWorker, BaseWorkerThread):
    def __init__(self, controller):
        BaseEventWorker.__init__(self)
        BaseWorkerThread.__init__(self, controller)

    def can_run_request(self, request):
        return True


class SimpleEventWorkerThread(SimpleWorker, BaseEventWorkerThread):
    def __init__(self, controller, objective):
        SimpleWorker.__init__(self, objective)
        BaseEventWorkerThread.__init__(self, controller)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            self.finish_success(record, value)
            logger.debug("Worker finished feval successfully")
        except Exception:
            self.finish_cancelled(record)
            logger.debug("Worker feval exited with exception")


class ProcessEventWorkerThread(ProcessWorker, BaseEventWorkerThread, BaseEventWorker):
    def __init__(self, controller):
        ProcessWorker.__init__(self, BaseEventWorkerThread)
        BaseEventWorkerThread.__init__(self, controller)
        BaseEventWorker.__init__(self)


class BaseSocketWorker(SocketWorker):
    def __init__(self, sockname, retries=0):
        SocketWorker.__init__(self, sockname, retries)

    def receive_request(self, timeout=1):
        ready = select.select([self.sock], [], [], timeout)
        if ready[0]:
            data = self.sock.recv(4096)
            return self.unmarshall(data)
        else:
            return None

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


class BaseEventSocketWorker(BaseEventWorker, BaseSocketWorker):
    def __init__(self, sockname, retries=0):
        BaseEventWorker.__init__(self)
        BaseSocketWorker.__init__(self, sockname, retries)

    def can_run_request(self, request):
        return True


class SimpleEventSocketWorker(SimpleWorker, BaseEventSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleWorker.__init__(self, objective)
        BaseEventSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        try:
            msg = ('complete', record_id, self.objective(*params))
        except Exception:
            logger.error(traceback.format_exc(5))
            msg = ('cancel', record_id)
        logger.debug("SENDING: {0}".format(msg))
        self.send(*msg)


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
        evalThread = Thread(target=interruptible_eval, args=(evalResults,))
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


class BaseInterruptibleWorkerThread(BaseInterruptibleWorker, BaseEventWorkerThread):
    def __init__(self, controller):
        """Initialize the EventWorker."""
        BaseInterruptibleWorker.__init__(self)
        BaseEventWorkerThread.__init__(self, controller)


class SimpleInterruptibleWorkerThread(SimpleWorker, BaseInterruptibleWorkerThread):
    def __init__(self, controller, objective):
        SimpleWorker.__init__(self, objective)
        BaseInterruptibleWorkerThread.__init__(self, controller)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            logger.debug("Worker finished feval successfully")
            return [self.finish_success, record, value]
        except Exception:
            logger.debug("Worker feval exited with exception")
            return [self.finish_cancelled, record]


class ProcessInterruptibleWorkerThread(ProcessWorker, BaseInterruptibleWorkerThread):
    def __init__(self, controller):
        ProcessWorker.__init__(self, BaseInterruptibleWorkerThread)
        BaseInterruptibleWorkerThread.__init__(self)


class BaseInterruptibleSocketWorker(BaseInterruptibleWorker, BaseEventSocketWorker):
    def __init__(self, sockname, retries=0):
        """Initialize the EventWorker."""
        BaseInterruptibleWorker.__init__(self)
        BaseEventSocketWorker.__init__(self, sockname, retries)


class SimpleInterruptibleSocketWorker(SimpleWorker, BaseInterruptibleSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleWorker.__init__(self, objective)
        BaseInterruptibleSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        try:
            msg = ('complete', record_id, self.objective(*params))
        except Exception:
            logger.error(traceback.format_exc(5))
            msg = ('cancel', record_id)
        logger.debug("SENDING: {0}".format(msg))
        return (self.send,) + msg
