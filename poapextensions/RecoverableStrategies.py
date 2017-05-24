from poap.strategy import RetryStrategy, BaseStrategy
import logging


# Get module-level logger
logger = logging.getLogger(__name__)


class RecoverableRetryStrategy(RetryStrategy):

    def __init__(self):
        RetryStrategy.__init__(self)

    def rput(self, proposal):
        """Put a retry proposal in the queue."""
        if not hasattr(proposal, 'retry'):
            logger.debug("Setting up retry")
            self._set(proposal, proposal.copy())
            proposal.retry = True  # Bugfix from POAP.strategy.RetryStrategy
            if proposal.action == 'eval':
                proposal.add_callback(self.on_reply)
                self.num_eval_pending += 1
            elif proposal.action == 'kill':
                proposal.add_callback(self.on_kill_reply)
            elif proposal.action == 'terminate':
                proposal.add_callback(self.on_terminate_reply)
        else:
            logger.debug("Skip retry setup -- already under management")
        self.put(proposal)

    def _resubmit(self, key):
        """Recycle a previously-submitted retry proposal with recovery information."""
        logger.debug("Resubmitting retry proposal")
        proposal = self._pop(key)
        if hasattr(key, 'recoveryInfo'):
            proposal.recoveryInfo = key.recoveryInfo
        self.rput(proposal)


class RecoverableFixedSampleStrategy(BaseStrategy):
    """Sample at a fixed set of points.

    Retries proposals that are preempted.

    The fixed sampling strategy is appropriate for any non-adaptive
    sampling scheme.  Since the strategy is non-adaptive, we can
    employ as many available workers as we have points to process.  We
    keep trying any evaluation that fails, and suggest termination
    only when all evaluations are complete.  The points in the
    experimental design can be provided as any iterable object (e.g. a
    list or a generator function).  One can use a generator for an
    infinite sequence if the fixed sampling strategy is used in
    combination with a strategy that provides a termination criterion.
    """

    def __init__(self, points):
        """Initialize the sampling scheme.

        Args:
            points: Points list or generator function.
        """
        def point_generator():
            """Generator wrapping the points list."""
            for point in points:
                yield point
        self.point_generator = point_generator()
        self.retry = RecoverableRetryStrategy()

    def propose_action(self):
        """Propose an action based on outstanding points."""
        try:
            if self.retry.empty():
                logger.debug("Getting new point from fixed schedule")
                point = next(self.point_generator)
                proposal = self.propose_eval(point)
                self.retry.rput(proposal)
            return self.retry.get()
        except StopIteration:
            logger.debug("Done fixed schedule; {0} outstanding evals".format(
                self.retry.num_eval_outstanding))
            if self.retry.num_eval_outstanding == 0:
                return self.propose_terminate()
