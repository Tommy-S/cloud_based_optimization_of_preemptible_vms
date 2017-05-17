import GPy
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm

# ~~~~WARNING: ONLY SUPPORT FOR 1D RIGHT NOW~~~~ # 

# TODO
def batch():
	pass

# Calculates GP from input and output vectors X and Y respectively
def calcGP(X, Y, kernel='rbf', variance=1., lengthscale=1.):
	# Reshape in 1D Case to proper column vector form
	if(len(X.shape) == 1):
		X = np.reshape(X, (len(X),1))
	if(len(Y.shape) == 1):
		Y = np.reshape(Y, (len(Y),1))


	if(kernel=='rbf'):
		kernel = GPy.kern.RBF(input_dim=1, variance=variance, lengthscale=lengthscale)
		m = GPy.models.GPRegression(X,Y,kernel)
		return m
	else:
		print('Kernel is not supported, please use one that is supported or use the default RBF Kernel')
		return None

# Updates GP with a set of new function evaluations Y at points X
def updateGP(model, kernel, Xnew, Ynew):
	# Reshape in 1D Case
	if(len(Xnew.shape) == 1):
		Xnew = np.reshape(X, (len(Xnew),1))
	if(len(Ynew.shape) == 1):
		Ynew = np.reshape(Y, (len(Ynew),1))
	X = np.append(model.X, Xnew, 0)
	Y = np.append(model.Y, Ynew, 0)	
	m = GPy.models.GPRegression(X,Y,kernel)
	return m


# Using Expected Improvement, send out a number of further evaluations
#	-batchsize = number of new evals
#	-fidelity = number of points used to estimate EI
#	-bounds = determines how new evals points are spaced
def batchNewEvals_EI(model, bounds=1, batchsize=50, fidelity=100):
	P, ei = compute_ei(model, fidelity)
	idx = np.argmax(ei)	
	xnew = P[idx]
 	X = np.linspace(xnew-bounds, xnew+bounds, num=batchsize)
	return X

# Calculates EI given means mu and variances sigma2
def compute_ei_inner(ybest, mu, sigma2):
	sigma = np.sqrt(sigma2)
	u = (ybest - mu) / sigma
	ucdf = norm.cdf(u)
	updf = norm.pdf(u)
	ei = sigma * (updf + u * ucdf)
	return ei

# Takes in GP model from GPy and computes EI at points P
# We are assuming minimization, and thus ybest represents the smallest point we have so far
def compute_ei(model, numsamples):
	P = np.linspace(model.X[0], model.X[-1], num=numsamples)
	ybest = np.amax(model.Y)
	P = np.reshape(P, [len(P), 1])
	mu, sigma2 = model.predict(P)
	return P, compute_ei_inner(ybest, mu, sigma2)

def plotGP(model):	
	fig = model.plot()
	regfig = GPy.plotting.show(fig)
	regfig.savefig('GPmodel.png')
