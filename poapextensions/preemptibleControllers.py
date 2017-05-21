from poap.controller import ThreadController
from poap.tcpserve import ThreadedTCPServer
from poapextensions.SocketWorkerHandlers import (
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
            logger.debug("Reject eval proposal -- no worker")
            proposal.reject()


class PreemptibleThreadedTCPServer(ThreadedTCPServer):
    def __init__(self, sockname=("localhost", 0), strategy=None, handlers={}):
        super(ThreadedTCPServer, self).__init__(sockname, PreemptibleSocketWorkerHandler)
        handlers['eval_preempted'] = self.handle_eval_preempt
        handlers['exit_preempted'] = self.handle_exit_preempt
        self.message_handlers = handlers
        self.controller = PreemptibleThreadController()
        self.controller.strategy = strategy
        self.controller.add_term_callback(self.shutdown)
        # self.daemon_threads = True

    def handle_eval_preempt(self, record):
        self.controller.add_message(lambda: record.cancel)

    def handle_exit_preempt(self):
        logger.debug("Server recognizes that worker was preempted")
        return


class RecoverableThreadedTCPServer(ThreadedTCPServer):
    def __init__(self, sockname=("localhost", 0), strategy=None, handlers={}):
        super(ThreadedTCPServer, self).__init__(sockname, RecoverableSocketWorkerHandler)
        handlers['eval_preempted'] = self.handle_eval_preempt
        handlers['exit_preempted'] = self.handle_exit_preempt
        self.message_handlers = handlers
        self.controller = RecoverableTCPThreadController()
        self.controller.strategy = strategy
        self.controller.add_term_callback(self.shutdown)
        # self.daemon_threads = True

    def handle_eval_preempt(self, record, recoveryInfo=None):
        if recoveryInfo is not None:
            record.recoveryInfo = recoveryInfo
        self.controller.add_message(lambda: record.cancel)

    def handle_exit_preempt(self):
        logger.debug("Server recognizes that worker was preempted")
        return
