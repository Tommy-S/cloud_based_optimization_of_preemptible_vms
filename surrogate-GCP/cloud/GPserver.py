import threading
import time
import GPy
import numpy as np
from cloudUnit import cloudUnit
from poap.tcpserve import ThreadedTCPServer
from poap.strategy import FixedSampleStrategy
from poap.strategy import InputStrategy

# Start Up TCP Server
strategy = FixedSampleStrategy([])
server = ThreadedTCPServer(sockname=('0.0.0.0', 3000))
tstrategy = InputStrategy(server.controller, strategy)
server.strategy = tstrategy

# Evaluate points in initial sweep
numevals = 15
X = np.linspace(-3, 3, num=numevals)
for x in X:
	tstrategy.eval(x)

# Get Server Socket
name = server.sockname 
print "Starting Server on socket ", name

# Start CloudUnit Worker
unit = cloudUnit('xenon-marker-147522')
unit.startWorker()
print "Worker Successfully Started"

# Start Server
cthread = threading.Thread(target=server.run)
cthread.start()

# Busy Wait (to be changed to semaphore later)
timeout = 0
while(timeout < 10):
	if(len(server.controller.fevals) > numevals - 10):
		print "Check complete, first round of evaluations finished, check number: ", \
		server.controller.fevals[numevals-10].value
		break
	else:
		time.sleep(5)
		timeout = timeout + 1

# Add more function evals on the controller queue
# ~~Note that if function evaluations occur too quickly, further evaluations will not
# ~~occur as the workers will be sent a finish signal first  
X = np.linspace(3, 6, num=numevals)
for x in X:
	tstrategy.eval(x)

cthread.join()
offset = 10;
Xnew = np.zeros([len(server.controller.fevals)-offset, 1])
Ynew = np.zeros([len(server.controller.fevals)-offset, 1])
for k in range(len(server.controller.fevals)-offset):
	Ynew[k] = server.controller.fevals[k].value
	Xnew[k] = server.controller.fevals[k].params
kernel = GPy.kern.RBF(input_dim=1, variance=1., lengthscale=1.)
m = GPy.models.GPRegression(Xnew,Ynew,kernel)
result = server.controller.best_point()
print("Final: {0:.3e} @ {1}".format(result.value, result.params))
