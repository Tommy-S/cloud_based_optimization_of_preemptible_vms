import sys
import time
import logging
import threading
import GPy
import numpy as np
import matplotlib.pyplot as plt
import pdb
from GPhelpers import *
from IPython.display import display
from poap.strategy import FixedSampleStrategy
from poap.strategy import InputStrategy
from poap.tcpserve import ThreadedTCPServer
from poap.tcpserve import SimpleSocketWorker
from scipy.stats import norm


# Set up default host, port, and time
TIMEOUT = .2


def f(x):
	logging.info("Request for {0}".format(x))
	if TIMEOUT > 0:
		time.sleep(TIMEOUT)
	logging.info("OK, done")
	return 5*np.sin(x)


def worker_main(name):
	logging.info("Launching worker on port {0}".format(name[1]))
	SimpleSocketWorker(f, sockname=name, retries=1).run()


def main():
	logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",
						level=logging.INFO)

	# Launch controller, server
	strategy = FixedSampleStrategy([])
	server = ThreadedTCPServer()
	initbatchsize = 20
	tstrategy = InputStrategy(server.controller, strategy);
	X = np.random.uniform(-3.,3.,(initbatchsize,1))
	Y = np.ones([len(X), 1])
	for k in X:
		tstrategy.eval(k)
	server.strategy = tstrategy
	cthread = threading.Thread(target=server.run)
	cthread.start()

	# Get controller port
	name = server.sockname
	logging.info("Launch controller at {0}".format(name))

	# Launch workers on local machine
	numworkers = 5;
	wthreads = []
	for k in range(numworkers):
		wthread = threading.Thread(target=worker_main, args=(name,))
		wthread.start()
		wthreads.append(wthread)

	# Wait for some fevals to complete
	time.sleep(.5)
	
	# Main Loop
	batchsize = 20; numfevals = 0; maxfevals = 80
	while(numfevals < maxfevals):
		# Get new fevals
		offset = numworkers
		numfevals = len(server.controller.fevals)
		Xnew = np.zeros([numfevals-offset, 1])
		Ynew = np.zeros([numfevals-offset, 1])
		for k in range(len(server.controller.fevals)-offset):
			Ynew[k] = server.controller.fevals[k].value[0]
			Xnew[k] = server.controller.fevals[k].params[0]
		
		# Calculate GP and batch out new fevals
		m = calcGP(Xnew, Ynew)
		X = batchNewEvals_EI(m, bounds=1, batchsize=batchsize, fidelity=100)
		for k in X:
			tstrategy.eval([k])
		
		# Wait for some fevals to complete
		time.sleep(.5)
	
	# Plot and wait on controller and workers
	plotGP(m)
	cthread.join()
	for t in wthreads:
		t.join()

if __name__ == '__main__':
	if len(sys.argv) > 1:
		TIMEOUT = float(sys.argv[1])
	main()
