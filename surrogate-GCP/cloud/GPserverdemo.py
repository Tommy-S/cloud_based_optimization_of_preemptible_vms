import threading
import time
import GPy
import numpy as np
from matplotlib import pyplot as plt
from cloudUnit import cloudUnit
from poap.tcpserve import ThreadedTCPServer
from poap.strategy import FixedSampleStrategy
from poap.strategy import InputStrategy

# Evaluate points in initial sweep
numevals = 30
X = 4*np.random.random(numevals)-2

# Start Up TCP Server
strategy = FixedSampleStrategy(X)
server = ThreadedTCPServer(sockname=('0.0.0.0', 3000), strategy=strategy)

# Get Server Socket
name = server.sockname 
print "Starting Server on socket ", name

# Start CloudUnit Worker
unit = cloudUnit('xenon-marker-147522')
unit.startWorker()
print "Worker Successfully Started..."

# Start Server
cthread = threading.Thread(target=server.run)
cthread.start()
time.sleep(35)
plt.ion()
plt.show()
while(len(server.controller.fevals) < numevals):
	try:
		time.sleep(1.5)
		Xnew = np.zeros([len(server.controller.fevals), 1])
		Ynew = np.zeros([len(server.controller.fevals), 1])
		for k in range(len(server.controller.fevals)):
			Ynew[k] = server.controller.fevals[k].value
			Xnew[k] = server.controller.fevals[k].params
		idx = np.isnan(Ynew)
		idx = np.nonzero(idx)
		Xnew = np.delete(Xnew, idx)
		Ynew = np.delete(Ynew, idx)
		Ynew = np.reshape(Ynew, [len(Ynew), 1])
		Xnew = np.reshape(Xnew, [len(Xnew), 1])
		kernel = GPy.kern.RBF(input_dim=1, variance=1., lengthscale=1.)
		m = GPy.models.GPRegression(Xnew,Ynew,kernel)
		m.optimize_restarts(10)
		fig = m.plot()
		plt.pause(.5)
#		m.optimize_restarts(3)
	except:
		print('Waiting...')
cthread.join()
result = server.controller.best_point()
unit.cleanupWorkers()
print("Final: {0:.3e} @ {1}".format(result.value, result.params))
