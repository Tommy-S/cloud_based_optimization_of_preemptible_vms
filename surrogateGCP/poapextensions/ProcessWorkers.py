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


class ProcessEvaluator(object):
    def __init__(self, workerType):
        self.workerType = workerType
        self.process = None

    def _kill_process(self):
        if self.process_is_running():
            logger.debug("ProcessEvaluator is killing subprocess")
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


class ProcessEventThreadWorker(ProcessEvaluator, EventThreadWorker):
    def __init__(self, controller):
        ProcessEvaluator.__init__(self, EventThreadWorker)
        EventThreadWorker.__init__(self, controller)


class ProcessInterruptibleThreadWorker(ProcessEvaluator, InterruptibleThreadWorker):
    def __init__(self, controller):
        ProcessEvaluator.__init__(self, InterruptibleThreadWorker)
        InterruptibleThreadWorker.__init__(self)


class ProcessPreemptibleThreadWorker(ProcessEvaluator, PreemptibleThreadWorker):
    def __init__(self, controller):
        ProcessEvaluator.__init__(self, PreemptibleThreadWorker)
        PreemptibleThreadWorker.__init__(self)
