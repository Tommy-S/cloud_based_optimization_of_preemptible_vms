from poap.controller import ThreadController
try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
import pickle
import threading
from poapextensions.SocketWorkerHandlers import (
    SocketWorkerHandler,
    PreemptibleSocketWorkerHandler,
    RecoverableSocketWorkerHandler,
)
try:
    import Queue
except ImportError:
    import queue as Queue
import logging

# Get module-level logger
logger = logging.getLogger(__name__)


class PreemptibleThreadController(ThreadController):
    """Add handling of dead workers to ThreadController."""

    def _submit_work(self, proposal):
        """Submit proposed work."""
        try:
            worker = self.workers.get_nowait()
            if worker.is_alive():
                logger.debug("Accept eval proposal")
                proposal.record = self.new_feval(proposal.args)
                proposal.record.worker = worker
                proposal.accept()
                worker.eval(proposal.record)
            else:
                self._submit_work(proposal)
        except Queue.Empty:
            logger.debug("Reject eval proposal -- no worker")
            proposal.reject()


class RecoverableTCPThreadController(ThreadController):
    """Add worker recovery to ThreadController."""

    def _submit_work(self, proposal):
        """Submit proposed work."""
        try:
            worker = self.workers.get_nowait()
            if worker.is_alive():
                logger.debug("Accept eval proposal")
                proposal.record = self.new_feval(proposal.args)
                proposal.record.worker = worker
                if hasattr(proposal, 'recoveryInfo'):
                    proposal.record.recoveryInfo = proposal.recoveryInfo
                proposal.accept()
                worker.eval(proposal.record)
            else:
                self._submit_work(proposal)
        except Queue.Empty:
            proposal.reject()


class ThreadedTCPServer(socketserver.ThreadingMixIn,
                        socketserver.TCPServer, object):
    """SocketServer interface for workers to connect to controller.

    The socket server interface lets workers connect to a given
    TCP/IP port and exchange updates with the controller.

    The server sends messages of the form

        ('eval', record_id, args, extra_args)
        ('eval', record_id, args)
        ('kill', record_id)
        ('terminate')

    The default messages received are

        ('running', record_id)
        ('kill', record_id)
        ('cancel', record_id)
        ('complete', record_id, value)

    The set of handlers can also be extended with a dictionary of
    named callbacks to be invoked whenever a record update comes in.
    For example, to set a lower bound field, we might use the handler

        def set_lb(rec, value):
            rec.lb = value
        handlers = {'lb' : set_lb }

    This is useful for adding new types of updates without mucking
    around in the EvalRecord implementation.

    Attributes:
        controller: ThreadController that manages the optimization
        handlers: dictionary of specialized message handlers
        strategy: redirects to the controller strategy
        newConnectionCallbacks: functions to execute when a new
            SocketWorkerHandler is created.
    """

    def __init__(
        self,
        sockname=("localhost", 0),
        strategy=None,
        handlers={},
        socketWorkerHandler=SocketWorkerHandler,
        controller=ThreadController,
        newConnectionCallbacks=[]
    ):
        """Initialize the controller on the given (host,port) address.

        Args:
            sockname: Socket on which to serve workers
            strategy: Strategy object to connect to controllers
            handlers: Dictionary of specialized message handlers
        """
        super(ThreadedTCPServer, self).__init__(sockname, socketWorkerHandler)
        self.message_handlers = handlers
        self.controller = controller()
        self.controller.strategy = strategy
        self.controller.add_term_callback(self.shutdown)
        self.newConnectionCallbacks = newConnectionCallbacks

    def marshall(self, *args):
        """Convert an argument list to wire format."""
        return pickle.dumps(args)

    def unmarshall(self, data):
        """Convert wire format back to Python arg list."""
        return pickle.loads(data)

    @property
    def strategy(self):
        return self.controller.strategy

    @strategy.setter
    def strategy(self, strategy):
        self.controller.strategy = strategy

    @property
    def sockname(self):
        return self.socket.getsockname()

    def run(self, merit=lambda r: r.value, filter=None):
        thread = threading.Thread(target=self.controller.run)
        thread.start()
        self.serve_forever()
        thread.join()
        return self.controller.best_point(merit=merit, filter=filter)

    def handle_new_connection(self, clientIP, socketWorkerHandler):
        for callback in self.newConnectionCallbacks:
            callback(clientIP, socketWorkerHandler)


class PreemptibleThreadedTCPServer(ThreadedTCPServer):
    def __init__(
        self,
        sockname=("localhost", 0),
        strategy=None,
        handlers={},
        socketWorkerHandler=PreemptibleSocketWorkerHandler,
        controller=PreemptibleThreadController,
        newConnectionCallbacks=[]
    ):
        handlers['eval_preempted'] = self.handle_eval_preempt
        handlers['exit_preempted'] = self.handle_exit_preempt

        super(PreemptibleThreadedTCPServer, self).__init__(
            sockname=sockname,
            strategy=strategy,
            handlers=handlers,
            socketWorkerHandler=socketWorkerHandler,
            controller=controller,
            newConnectionCallbacks=newConnectionCallbacks
        )
        self.daemon_threads = True

    def handle_eval_preempt(self, record):
        self.controller.add_message(lambda: record.cancel)

    def handle_exit_preempt(self):
        logger.debug("Server recognizes that worker was preempted")
        return


class RecoverableThreadedTCPServer(PreemptibleThreadedTCPServer):
    def __init__(
        self,
        sockname=("localhost", 0),
        strategy=None,
        handlers={},
        socketWorkerHandler=RecoverableSocketWorkerHandler,
        controller=RecoverableTCPThreadController,
        newConnectionCallbacks=[]
    ):
        super(RecoverableThreadedTCPServer, self).__init__(
            sockname=sockname,
            strategy=strategy,
            handlers=handlers,
            socketWorkerHandler=socketWorkerHandler,
            controller=controller,
            newConnectionCallbacks=newConnectionCallbacks
        )

    def handle_eval_preempt(self, record, recoveryInfo=None):
        if recoveryInfo is not None:
            record.recoveryInfo = recoveryInfo
        PreemptibleThreadedTCPServer.handle_eval_preempt(self, record)
