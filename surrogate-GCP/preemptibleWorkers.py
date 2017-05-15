from poap.controller import BasicWorkerThread
from poap.tcpserve import SocketWorker, ProcessSocketWorker, SocketWorkerHandler
import logging
from threading import Thread, Semaphore
import time
import socket
from Queue import Empty
import traceback

TIMEOUT = 0
# Get module-level logger
logger = logging.getLogger(__name__)


class PreemptibleWorker(object):
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
    (2) eval(self, params) : unit
    (3) finish_preempted(self, params) : unit

    """

    def __init__(self):
        """Initialize the PreemptibleWorker."""
        # True when a preemption event has been detected.
        # Should only be modified by the preemption detection thread.
        self.preempted = False

        # Released on detection of a preemption event or
        # evaluation completion
        self.killable = Semaphore(1)

        def detect_preemption():
            """
            Repeatedly check for preemption events.
            If one has been found, set the preemption flag and release the
            semaphore to interrupt any ongoing optimization function evaluation.
            """
            while not self.is_preempted():
                time.sleep(1)
            self.preempted = True
            self.killable.release()
            logger.debug("Worker detected preemption event")

        # Detects preemption events. Is a daemon so the worker
        # may shutdown without cleaning up preemption detection
        self.preemptionThread = Thread(target=detect_preemption)
        self.preemptionThread.daemon = True
        self.preemptionThread.start()

    def is_preempted(self):
        """
        Check computation environment for preemption events.
        Returns a boolean that is true iff a preemption event has occurred.
        """
        return False

    def eval(self, *params):
        """
        Evaluate the optimization function and package the results for handling.
        Should only be called from the preemptible evaluation thread.
        Eval itself should not communicate with the controller.
        Communication with the controller is done in the function callback that
        eval returns- this is to prevent interleaving a preemption message into
        eval completion messages.

        Args:
            params: placeholder function signature
                    Must have the same signature as finish_preempted.

        Returns:
            A tuple or list containing a callback function and its arguments.
            return[0] is a function object
            return[1:] are the arguments to that function
        """
        return (lambda: None,)

    def finish_preempted(self, params):
        """
        Clean up interrupted evaluation of the optimization function.
        Should only be called from the root worker thread when a
        preemption event interrupts normal evaluation.

        Args:
            params: placeholder function signature
                    Must have the same signature as eval.
        """
        return

    def preemptible_eval(self, *params):
        """
        Wrap optimization function evaluation so it can be canceled in case of a preemption event.

        Args:
            params: passed through to eval and finish_preempted
        """
        def _preemptible_eval(evalResults):
            evalResults[:] = self.eval(*params)
            self.killable.release()

        evalResults = []
        evalThread = Thread(target=_preemptible_eval, args=(evalResults,))
        evalThread.daemon = True
        self.killable.acquire()  # To be released at the end of evalThread
        # DON'T START evalThread UNTIL SEMAPHORE IS ACQUIRED OTHERWISE RACE CONDITION
        evalThread.start()

        # Acquiring happens when one of either preemption detection or eval threads returns
        self.killable.acquire()

        if evalThread.is_alive():
            # Computation was preempted
            self.finish_preempted(*params)
            logger.debug("Worker feval preempted")
        else:
            logger.debug("Successful computation")
            evalThread.join()
            logger.debug("Joined evalThread")
            method = evalResults[0]
            method(*evalResults[1:])
            self.killable.release()  # Free up semaphore for next evaluation


class PreemptibleBasicWorkerThread(BasicWorkerThread, PreemptibleWorker):
    """Basic preemptible worker for use with the thread controller.

    The PreemptibleBasicWorkerThread calls a Python objective function
    when asked to do an evaluation.  This is concurrent, but only
    results in parallelism if the objective function implementation
    itself allows parallelism (e.g. because it communicates with
    an external entity via a pipe, socket, or whatever).
    Execution of the function can be interrupted at any point.
    """

    def __init__(self, controller, objective):
        BasicWorkerThread.__init__(self, controller, objective)
        PreemptibleWorker.__init__(self)

    def eval(self, record):
        """
        Evaluate the optimization function and handle the results.

        Args:
            record: EvalRecord for this evaluation
        Must have the same signature as finish_preempted.
        """
        try:
            value = self.objective(*record.params)
            logger.debug("Worker finished feval successfully")
            return [self.finish_success, record, value]
        except Exception:
            logger.debug("Worker feval exited with exception")
            return [self.finish_cancelled, record]

    def finish_preempted(self, record):
        """Cancel evaluation record on preemption event."""
        self.add_message(record.cancel)
        logger.debug("Feval preempted")

    def handle_preempt(self):
        """Process preemption."""
        # TODO: empty self.queue
        logger.info("Worker exiting from preemption")

    def handle_terminate(self):
        """Terminate worker."""
        logger.info("Worker exiting from terminate")

    def run(self):
        """Run requests as long as we get them iff we have not been preempted."""
        while True:
            if self.preempted:
                logger.debug("Worker thread received preempt request")
                self.handle_preempt()
                return
            else:
                try:
                    request = self.queue.get(True, 1)
                    if request[0] == 'eval':
                        logger.debug("Worker thread received eval request")
                        record = request[1]
                        self.add_message(record.running)
                        self.preemptible_eval(record)
                    elif request[0] == 'kill':
                        logger.debug("Worker thread received kill request")
                        self.handle_kill(request[1])
                    elif request[0] == 'terminate':
                        logger.debug("Worker thread received terminate request")
                        self.handle_terminate()
                        logger.debug("Exit worker thread run()")
                        return
                except Empty:
                    pass


class PreemptibleSocketWorker(SocketWorker, PreemptibleWorker):
    def __init__(self, sockname, retries=0):
        """Initialize the PreemptibleSocketWorker.

        The constructor tries to open the socket; on failure, it keeps
        trying up to retries times, once per second.

        Args:
            sockname: (host, port) tuple where server lives
            retries: number of times to retry the connection
        """
        SocketWorker.__init__(self, sockname, retries)
        PreemptibleWorker.__init__(self)

    def handle_preempt(self):
        """Process preemption."""
        logger.info("Worker exiting from preemption")
        self.running = False
        msg = ('exit_preempted',)
        self.send(*msg)

    def _run(self):
        """Run a message from the controller."""
        if self.preempted:
            self.handle_preempt()
            return
        data = self.unmarshall(self.sock.recv(4096))
        if data[0] == 'eval':
            method = getattr(self, 'preemptible_eval')
        else:
            method = getattr(self, data[0])
        method(*data[1:])


class PreemptibleSimpleSocketWorker(PreemptibleSocketWorker):
    """Simple preemptible socket worker that runs a local objective function.

    The PreemptibleSimpleSocketWorker is a socket worker that runs a
    local Python function and returns the result.  It is probably mostly
    useful for testing -- the ProcessSocketWorker is a better option for
    external simulations.
    """

    def __init__(self, objective, sockname, retries=0):
        """Initialize the PreemptibleSimpleSocketWorker.

        The constructor tries to open the socket; on failure, it keeps
        trying up to retries times, once per second.

        Args:
            objective: Python objective function
            sockname: (host, port) tuple where server lives
            retries: number of times to retry the connection
        """
        PreemptibleSocketWorker.__init__(self, sockname, retries)
        self.objective = objective

    def finish_preempted(self, record_id, params):
        msg = ('eval_preempted', record_id)
        self.send(*msg)

    def eval(self, record_id, params):
        """Evaluate the function and send back a result.

        If the function evaluation crashes, we send back a 'cancel'
        request for the record.  If, on the other hand, there is a
        problem with calling send, we probably want to let the worker
        error out.

        Args:
            record_id: Feval record identifier used by server/controller
            params: Parameters sent to the function to be evaluated
        """
        try:
            msg = ('complete', record_id, self.objective(*params))
        except Exception:
            logger.error(traceback.format_exc(5))
            msg = ('cancel', record_id)
        return (self.send,) + msg


class PreemptibleProcessSocketWorker(PreemptibleSocketWorker, ProcessSocketWorker):
    """Socket worker that runs an evaluation in a subprocess.

    The ProcessSocketWorker is a base class for simulations that run a
    simulation in an external subprocess.  This class provides functionality
    just to allow graceful termination of the external simulations.

    Attributes:
        process: Handle for external subprocess
    """

    def __init__(self, sockname, retries=0):
        """Initialize the PreemptibleProcessSocketWorker.

        The constructor tries to open the socket; on failure, it keeps
        trying up to retries times, once per second.

        Args:
            sockname: (host, port) tuple where server lives
            retries: number of times to retry the connection
        """
        PreemptibleSocketWorker.__init__(self, sockname, retries)
        ProcessSocketWorker.__init__(self, sockname, retries)

    def eval(self, record_id, params):
        """See poap.controller.ProcessSocketWorker for comments."""
        return


class PreemptibleSocketWorkerHandler(SocketWorkerHandler):
    def _handle_message(self, args):
        """Receive a record status message."""
        mname = args[0]
        controller = self.server.controller
        if mname == 'exit_preempted':
            self.exit_preempted()
            handler = self.server.message_handlers[mname]
            controller.add_message(lambda: handler())
        else:
            record = self.records[args[1]]
            if mname in self.server.message_handlers:
                handler = self.server.message_handlers[mname]
                controller.add_message(lambda: handler(record, *args[2:]))
            else:
                method = getattr(record, mname)
                controller.add_message(lambda: method(*args[2:]))
            if mname == 'complete' or mname == 'cancel' or mname == 'kill':
                if self.running:
                    logger.debug("Re-queueing worker")
                    controller.add_worker(self)

    def exit_preempted(self):
        logger.debug("Handler's Worker was preempted and has shut down")
        try:
            self.running = False
            self.request.close()
        except socket.error as e:
            logger.warning("In exit_preempted: {0}".format(e))

    def is_alive(self):
        return self.running
