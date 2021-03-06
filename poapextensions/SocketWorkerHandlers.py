from poap.tcpserve import SocketWorkerHandler
import socket
import logging
import threading
import traceback

# Get module-level logger
logger = logging.getLogger(__name__)


class PreemptibleSocketWorkerHandler(SocketWorkerHandler):
    """Add preemption handling to a SocketWorkerHandler."""

    def setup(self):
        logger.info("New connection established")

    def handle(self):
        """Main event loop called from SocketServer."""
        cthread = threading.current_thread()
        if len(cthread.name) >= 7 and cthread.name[0:7] == 'Thread-':
            cthread.name = "SocketWorkerHandler"
        self.server.handle_new_connection(self.client_address, self)
        self.request.settimeout(0.1)
        self.records = {}
        self.running = True
        self.server.controller.add_term_callback(self.terminate)
        try:
            self.server.controller.add_worker(self)
            while self.running:
                try:
                    data = self.request.recv(4096)
                    if not data:
                        return
                    args = self.server.unmarshall(data)
                    logger.debug("Received: {0}".format(args))
                    self._handle_message(args)
                except socket.timeout:
                    pass

        except socket.error as e:
            logger.debug(traceback.format_exc())
            logger.debug("Exiting worker: {0}".format(e))
        finally:
            for rec_id, record in self.records.items():
                self._cleanup(record)

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

    def terminate(self):
        """Send a termination request to a remote worker."""
        if not self.running:
            return
        logger.debug("Send terminate to worker")
        try:
            self.running = False
            self.request.send(self.server.marshall('terminate'))
            self.request.close()
        except socket.error as e:
            logger.warning("In terminate: {0}".format(e))

    def exit_preempted(self):
        logger.debug("Handler's Worker was preempted and has shut down")
        try:
            self.running = False
            self.request.close()
        except socket.error as e:
            logger.warning("In exit_preempted: {0}".format(e))

    def is_alive(self):
        return self.running


class RecoverableSocketWorkerHandler(PreemptibleSocketWorkerHandler):
    """Add worker recovery to a PreemptibleSocketWorkerHandler."""

    def eval(self, record):
        """Send an evaluation request to remote worker."""
        logger.debug("Send eval to worker")
        self.records[id(record)] = record
        try:
            m = []
            if hasattr(record, 'recoveryInfo'):
                m.append('recover')
                m.append(record.recoveryInfo)
            else:
                m.append('eval')
            m.append(id(record))
            m.append(record.params)
            if record.extra_args is not None:
                m.append(record.extra_args)
            self.request.send(self.server.marshall(*m))
        except Exception as e:
            logger.warning("In eval: {0}".format(e))
            self._cleanup(record)
