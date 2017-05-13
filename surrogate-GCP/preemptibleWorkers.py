from poap.controller import BasicWorkerThread
from poap.tcpserve import SocketWorker, ProcessSocketWorker, SocketWorkerHandler
import logging
from threading import Thread, Semaphore
import time
import sys

TIMEOUT = 0
# Get module-level logger
logger = logging.getLogger(__name__)


class PreemptibleWorker(object):
    def __init__(self, messageQueue=None):
        self.running = True
        self.killable = Semaphore(2)
        self.preempted = False
        self.preemptionThread = Thread(target=self.detectPreemption, args=(self.killable,))
        self.preemptionThread.daemon = True
        self.killable.acquire()  # To be released on detection of preemption
        self.preemptionThread.start()

    def isPreempted(self):
        # TODO: implement preemption detection
        return False

    def eval(self, params):
        pass

    def finish_preempted(self, params):
        pass

    def preempt(self):
        self.preempted = True

    def detectPreemption(self, semaphore):
        """Release a semaphore upon detection of a pre-emption event."""
        while not self.isPreempted():
            time.sleep(1)
        self.preempt()
        semaphore.release()

    def semaphoreEval(self, *params):
        def _semaphoreEval():
            self.eval(*params)
            self.killable.release()

        evalThread = Thread(target=_semaphoreEval)
        evalThread.daemon = True
        self.killable.acquire()  # To be released at the end of evalThread
        # DON'T START evalThread UNTIL SEMAPHORE IS ACQUIRED OTHERWISE RACE CONDITION
        evalThread.start()

        self.killable.acquire()  # Acquiring happens when preempted or eval is done
        if evalThread.is_alive():
            # Computation was preempted
            self.finish_preempted(*params)
            logger.debug("Worker feval was preempted")
        else:
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
        """Initialize the worker."""
        BasicWorkerThread.__init__(self, controller, objective)
        PreemptibleWorker.__init__(self, self.queue)

    def eval(self, record):
        try:
            # Successfully executed the objective
            value = self.objective(*record.params)
            self.finish_success(record, value)
            logger.debug("Worker finished feval successfully")
        except Exception:
            self.finish_cancelled(record)
            logger.debug("Worker feval exited with exception")

    def finish_preempted(self, record):
        """Finish recording cancelled on a record."""
        self.add_message(record.cancel)
        logger.debug("Feval preempted")

    def preempt(self):
        super(PreemptibleBasicWorkerThread, self).preempt()
        self.queue.put(('preempt',))

    def handle_preempt(self):
        """Process preemption."""
        logger.info("Worker exiting from preemption")

    def handle_terminate(self):
        """Terminate worker."""
        logger.info("Worker exiting from terminate")

    def add_message(self, message):
        "Send message to be executed at the controller."
        if self.running:
            BasicWorkerThread.add_message(self, message)

    def add_worker(self):
        "Add worker back to the work queue."
        if self.running:
            BasicWorkerThread.add_worker(self)

    def run(self):
        """Run requests as long as we get them."""
        while True:
            request = self.queue.get()
            if not self.preempted:
                if request[0] == 'eval':
                    logger.debug("Worker thread received eval request")
                    record = request[1]
                    self.add_message(record.running)
                    self.semaphoreEval(record)
                elif request[0] == 'kill':
                    logger.debug("Worker thread received kill request")
                    self.handle_kill(request[1])
                elif request[0] == 'terminate':
                    logger.debug("Worker thread received terminate request")
                    self.handle_terminate()
                    logger.debug("Exit worker thread run()")
                    return
            elif request[0] == 'preempt':
                logger.debug("Worker thread received preempt request")
                self.handle_preempt()
                logger.debug("Worker preempted")
                return


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

    def send(self, *args):
        if self.running:
            SocketWorker.send(self, *args)

    def _run(self):
        """Run a message from the controller."""
        if not self.running:
            return
        data = self.unmarshall(self.sock.recv(4096))
        if data[0] == 'eval':
            method = getattr(self, 'semaphoreEval')
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

    def __init__(self, objective, sockname, retries=0, threadID=0):
        """Initialize the PreemptibleSimpleSocketWorker.

        The constructor tries to open the socket; on failure, it keeps
        trying up to retries times, once per second.

        Args:
            objective: Python objective function
            sockname: (host, port) tuple where server lives
            retries: number of times to retry the connection
        """
        self.id = threadID
        PreemptibleSocketWorker.__init__(self, sockname, retries)
        self.objective = objective

    def finish_preempted(self, record_id, params):
        msg = ('preempted', record_id)
        logger.debug('Worker {0} was preempted'.format(self.id))
        self.send(*msg)
        self.running = False

    def preempt(self):
        super(PreemptibleSimpleSocketWorker, self).preempt()

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
        except:
            logger.debug('EXCEPTION: ' + sys.exc_info()[0])
            msg = ('cancel', record_id)
        self.send(*msg)


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
        pass


class PreemptibleSocketWorkerHandler(SocketWorkerHandler):
    def __init__(self, *args):
        self.alive = True
        SocketWorkerHandler.__init__(self, *args)

    def _handle_message(self, args):
        """Receive a record status message."""
        mname = args[0]
        if mname == 'preempted':
            logger.debug("Handler's Worker was preempted")
            self.alive = False

        record = self.records[args[1]]
        controller = self.server.controller
        if mname in self.server.message_handlers:
            handler = self.server.message_handlers[mname]
            controller.add_message(lambda: handler(record, *args[2:]))
        else:
            method = getattr(record, mname)
            controller.add_message(lambda: method(*args[2:]))
        if mname == 'complete' or mname == 'cancel' or mname == 'kill':
            if self.alive:
                logger.debug("Re-queueing worker")
                controller.add_worker(self)

    def is_alive(self):
        return self.alive
