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

# Get module-level logger
logger = logging.getLogger(__name__)


class SimpleEvaluator(object):
    def __init__(self, objective):
        self.objective = objective


def simple_thread_evaluate(self, record):
    try:
        value = self.objective(*record.params)
        logger.debug("Worker finished feval successfully")
        return [self.finish_success, record, value]
    except Exception:
        logger.debug("Worker feval exited with exception")
        return [self.finish_cancelled, record]


def simple_socket_evaluate(self, record_id, params):
    try:
        msg = ('complete', record_id, self.objective(*params))
    except Exception:
        msg = ('cancel', record_id)
    return (self.send,) + msg


class SimpleEventThreadWorker(SimpleEvaluator, EventThreadWorker):
    def __init__(self, controller, objective):
        SimpleEvaluator.__init__(self, objective)
        EventThreadWorker.__init__(self, controller)

    def evaluate(self, record):
        results = simple_thread_evaluate(self, record)
        results[0](*results[1:])


class SimpleEventSocketWorker(SimpleEvaluator, EventSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleEvaluator.__init__(self, objective)
        EventSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        results = simple_socket_evaluate(self, record_id, params)
        results[0](*results[1:])


class SimpleInterruptibleThreadWorker(SimpleEvaluator, InterruptibleThreadWorker):
    def __init__(self, controller, objective):
        SimpleEvaluator.__init__(self, objective)
        InterruptibleThreadWorker.__init__(self, controller)

    def evaluate(self, record):
        return simple_thread_evaluate(self, record)


class SimpleInterruptibleSocketWorker(SimpleEvaluator, InterruptibleSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleEvaluator.__init__(self, objective)
        InterruptibleSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        return simple_socket_evaluate(self, record_id, params)


class SimplePreemptibleThreadWorker(SimpleEvaluator, PreemptibleThreadWorker):
    def __init__(self, controller, objective):
        SimpleEvaluator.__init__(self, objective)
        PreemptibleThreadWorker.__init__(self, controller)

    def evaluate(self, record):
        return simple_thread_evaluate(self, record)


class SimplePreemptibleSocketWorker(SimpleEvaluator, PreemptibleSocketWorker):
    def __init__(self, objective, sockname, retries=0):
        SimpleEvaluator.__init__(self, objective)
        PreemptibleSocketWorker.__init__(self, sockname, retries)

    def evaluate(self, record_id, params):
        return simple_socket_evaluate(self, record_id, params)
