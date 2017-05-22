from poapextensions.SimpleWorkers import SimpleGCPRecoverableSocketWorker
from poapextensions.RecoveryStates import BasicLock, BasicStateObject
import time


def objective(x, stateLock, state):
    def initState():
        if 'count' not in state:
            stateLock.acquire()
            state['count'] = 0
            stateLock.release()
    initState()

    while state['count'] < 30:
        time.sleep(1)
        stateLock.acquire()
        state['count'] = state['count'] + 1
        stateLock.release()
    return (x + 1)


def run(hostip, port):
    SimpleGCPRecoverableSocketWorker(objective, BasicLock, BasicStateObject, (hostip, port), 1).run()
