from poap.tcpserve import SocketWorkerHandler
import socket
import logging

# Get module-level logger
logger = logging.getLogger(__name__)


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
