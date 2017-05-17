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


class GPsim:

	def __init__(self, batchsize=100, prunerate=.2, timebound=10, money=1000, fevalcost=1):
		self.batchsize = batchsize
		self.prunerate = prunerate
		self.timebound = timebound
		self.money = money
		self.fevalcost = fevalcost
		
	def run(self, f, bounds):

		breakcond = 1e-5

		# run initial batch, deduct money
		self.money = self.money - self.batchsize*self.fevalcost
		eval_logX = np.random.uniform(bounds[0], bounds[1], self.batchsize)
		eval_logY = f(eval_logX)
		ybest = np.amin(eval_logY)

		while(self.money > 0):
			# calc Gaussian Process
			m = calcGP(eval_logX, eval_logY)
			
			# calc batchsize, break if necessary
			self.batchsize = np.floor(self.batchsize*(1-self.prunerate))
			if(self.batchsize < 2):
				print "Batch Size reached Minimum"
				break
			
			# Deduct Money, evaluate new batch
			self.money = self.money - self.batchsize*self.fevalcost
			X = batchNewEvals_EI(m, bounds=1, batchsize=self.batchsize, fidelity=1000)
			Y = f(X)
			eval_logY = np.concatenate([eval_logY, Y])
			eval_logX = np.concatenate([eval_logX, X])

			ynew = np.amin(eval_logY)
			if(np.absolute(ynew - ybest) < breakcond):
				print "Break Condition Reached, Improvement Halted"
				print "Num evals:", eval_logY.size
				break


		plotGP(m)
		print 