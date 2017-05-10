import sys
import time
import logging
import argparse
import threading
from poap.tcpserve import SimpleSocketWorker

# Worker evaluates function f(x) for some given x. Takes in two arguments, an IP address and a Port:
# i.e. tcpworker 127.00.01 4000 
# where 127.00.01 is the IP address and 4000 is the port number. 

timeout = 3

def f(x):
	logging.info("Request for {0}".format(x))
	if timeout > 0:
		time.sleep(timeout)
	logging.info("OK, done")
	return (x-1.23)*(x-1.23)

def worker_main(name):
	logging.info("Launching worker on port {0}".format(name[1]))
	SimpleSocketWorker(f, sockname=name, retries=1).run()

def main():
	# Parse InputArgs
	logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s",level=logging.INFO)
	parser = argparse.ArgumentParser(description='Process some integers.')
	parser.add_argument('strings', metavar='N', type=str, nargs='+',)
	parser.add_argument('integers', metavar='N', type=int, nargs='+',)
	args = parser.parse_args()
	argint = args.integers
	argstr = args.strings
	IP = argstr[0];
	port = argint[0]

	# Launch worker with two threads 
	print "Launching worker on port ", port
	name = (IP, port)
	wthreads = []
	for k in range(2):
		wthread = threading.Thread(target=worker_main, args=(name,))
		wthread.start()
		wthreads.append(wthread)	
	for t in wthreads:
		t.join()

if __name__ == '__main__':
    main()
