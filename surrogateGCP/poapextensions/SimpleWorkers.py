from surrogateGCP.poapextensions.ThreadWorkers import (
    EventThreadWorker,
    InterruptibleThreadWorker,
    PreemptibleThreadWorker,
)
from surrogateGCP.poapextensions.SocketWorkers import (
    EventSocketWorker,
    InterruptibleSocketWorker,
    PreemptibleSocketWorker,
)
import logging
import traceback

# Get module-level logger
logger = logging.getLogger(__name__)


class SimpleEvaluator(object):
    def __init__(self, objective):
        self.objective = objective


class SimpleEventThreadWorker(SimpleEvaluator, EventThreadWorker):
    def __init__(self, controller, objective):
        SimpleEvaluator.__init__(self, objective)
        EventThreadWorker.__init__(self, controller)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            self.finish_success(record, value)
            logger.debug("Worker finished feval successfully")
        except Exception:
            self.finish_cancelled(record)
            logger.debug("Worker feval exited with exception")


class SimpleEventSocketWorker(SimpleEvaluator, EventSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleEvaluator.__init__(self, objective)
        EventSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        try:
            msg = ('complete', record_id, self.objective(*params))
        except Exception:
            logger.error(traceback.format_exc(5))
            msg = ('cancel', record_id)
        logger.debug("SENDING: {0}".format(msg))
        self.send(*msg)


class SimpleInterruptibleThreadWorker(SimpleEvaluator, InterruptibleThreadWorker):
    def __init__(self, controller, objective):
        SimpleEvaluator.__init__(self, objective)
        InterruptibleThreadWorker.__init__(self, controller)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            logger.debug("Worker finished feval successfully")
            return [self.finish_success, record, value]
        except Exception:
            logger.debug("Worker feval exited with exception")
            return [self.finish_cancelled, record]


class SimpleInterruptibleSocketWorker(SimpleEvaluator, InterruptibleSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleEvaluator.__init__(self, objective)
        InterruptibleSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        try:
            msg = ('complete', record_id, self.objective(*params))
        except Exception:
            logger.error(traceback.format_exc(5))
            msg = ('cancel', record_id)
        logger.debug("SENDING: {0}".format(msg))
        return (self.send,) + msg


class SimplePreemptibleThreadWorker(SimpleEvaluator, PreemptibleThreadWorker):
    def __init__(self, controller, objective):
        SimpleEvaluator.__init__(self, objective)
        PreemptibleThreadWorker.__init__(self, controller)

    def evaluate(self, record):
        try:
            value = self.objective(*record.params)
            return [self.finish_success, record, value]
        except Exception:
            return [self.finish_cancelled, record]


class SimplePreemptibleSocketWorker(SimpleEvaluator, PreemptibleSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleEvaluator.__init__(self)
        PreemptibleSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        try:
            msg = ('complete', record_id, self.objective(*params))
        except Exception:
            logger.error(traceback.format_exc(5))
            msg = ('cancel', record_id)
        logger.debug("SENDING: {0}".format(msg))
        return (self.send,) + msg
