from poap.controller import ThreadController
from poap.tcpserve import ThreadedTCPServer
from preemptibleWorkers import PreemptibleSocketWorkerHandler
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


class PreemptibleTCPThreadController(ThreadController):

    def _submit_work(self, proposal):
        """Submit proposed work."""
        try:
            worker = self.workers.get_nowait()
            if worker.is_alive():
                if worker.running:
                    logger.debug("Accept eval proposal")
                    proposal.record = self.new_feval(proposal.args)
                    proposal.record.worker = worker
                    proposal.accept()
                    worker.eval(proposal.record)
                else:
                    self.workers.put(worker)
                    self._submit_work(proposal)
            else:
                self._submit_work(proposal)
        except Queue.Empty:
            logger.debug("Reject eval proposal -- no worker")
            proposal.reject()


class PreemptibleThreadedTCPServer(ThreadedTCPServer):
    def __init__(self, sockname=("localhost", 0), strategy=None, handlers={}):
        super(ThreadedTCPServer, self).__init__(sockname, PreemptibleSocketWorkerHandler)
        handlers['preempted'] = self.handle_preempt
        self.message_handlers = handlers
        self.controller = PreemptibleTCPThreadController()
        self.controller.strategy = strategy
        self.controller.add_term_callback(self.shutdown)

    def handle_preempt(self, record):
        self.controller.add_message(lambda: record.cancel)
