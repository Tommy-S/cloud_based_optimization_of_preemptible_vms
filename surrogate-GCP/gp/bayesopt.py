# Code base helpfully provided by Jake Gardner: http://www.cs.cornell.edu/~jgardner/

import GPy
import numpy as np
import ghalton
import random
from scipy.stats import norm
import pdb

class BayesOpt:

    def __init__(self, func, initial_kernel, num_dims, grid_size, mins, maxes):
        self.func = func
        self.kernel = initial_kernel
        self.num_dims = num_dims
        self.grid_size = grid_size
        self.mins = mins
        self.maxes = maxes
        self.X = None
        self.y = None
        self.best_y = None
        self.best_x = None

    def initialize_run(self,num_iters=10,init_x=np.array([]),init_y=float('inf')):
        self.y = np.array([])
        self.X = np.array([])
        self.y.shape = (0,1)
        self.X.shape = (0,self.num_dims)
        if init_x.size != 0:
            scaled_x = self.scale_point(init_x)
            if init_y < float('inf'):
                self.y = np.concatenate((self.y, np.array([[init_y]])))
                self.X = np.concatenate((self.X, np.array([scaled_x])))
            else:
                val = self.func(init_x)
                print(val)
                self.y = np.concatenate((self.y, np.array([[val]])))
                self.X = np.concatenate((self.X, np.array([scaled_x])))
        for i in range(num_iters):
            random_candidate = self.next_candidate_discrete(np.array([]))
            true_x = self.unscale_point(random_candidate)
            val = self.func(true_x)
            print(val)
            self.y = np.concatenate((self.y, np.array([[val]])))
            self.X = np.concatenate((self.X, np.array([random_candidate])))
        
        best_index = np.argmin(self.y)
        self.best_y = self.y[best_index][0]
        self.best_x = self.unscale_point(self.X[best_index])

        # Create initial model
        self.model = GPy.models.GPRegression(self.X,self.y,self.kernel)
        self.model.optimize()

    # Chooses the next point to run in a manner that exploits the additive structure
    # in the model, if any exists.
    def next_candidate_dimscan(self,model):
        dim_groups = [k.active_dims for k in model.kern.parts]
        print kl2str(model.kern.parts)

        candidate_set = np.tile(self.scale_point(self.best_x), (self.grid_size, 1))

        if not self.best_y:
            randind = random.randrange(0,self.grid_size)
            return candidate_set[randind,:]

        for group in dim_groups:
            D = len(group)
            sequencer = ghalton.Halton(D)
            cand_subset = np.array(sequencer.get(self.grid_size))

            candidate_set[:,group] = cand_subset
            best_cand, ei_val = self.next_candidate_discrete(model,candidate_set=candidate_set)

            candidate_set = np.tile(best_cand, (self.grid_size, 1))

        return best_cand, ei_val

    def next_candidate_discrete(self,model,candidate_set=np.array([])):
        if candidate_set.size == 0:
            sequencer = ghalton.Halton(self.num_dims)
            candidate_set = np.array(sequencer.get(self.grid_size))

        if not self.best_y:
            randind = random.randrange(0,self.grid_size)
            return candidate_set[randind,:]

        mu, sigma2 = model.predict(candidate_set)
        ei = self.compute_ei(mu, sigma2)
        max_index = np.argmax(ei)
        return candidate_set[max_index,:], ei[max_index]

    def compute_ei(self, mu, sigma2):
        sigma = np.sqrt(sigma2)
        u = (self.best_y - mu) / sigma
        ucdf = norm.cdf(u)
        updf = norm.pdf(u)
        ei = sigma * (updf + u * ucdf)
        #print "EI mean/std", np.mean(ei), np.std(ei)
        return ei

    def unscale_point(self, x):
        return (self.maxes - self.mins) * x + self.mins

    def scale_point(self, x):
        return (x - self.mins) / (self.maxes - self.mins)

class FixedModelBayesOpt(BayesOpt):
    def initialize_run(self, num_iters=10, init_x=np.array([]), init_y = float('inf')):
        sequencer = ghalton.Halton(self.num_dims)
        self.candidate_set = np.array(sequencer.get(self.grid_size))
        BayesOpt.initialize_run(self,num_iters,init_x,init_y)


    def next_candidate_discrete(self,model):        
        if not self.best_y:
            randind = random.randrange(0,self.grid_size)
            return self.candidate_set[randind,:]

        mu, sigma2 = model.predict(self.candidate_set)
        ei = self.compute_ei(mu, sigma2)
        max_index = np.argmax(ei)
        next_pt, next_ei = self.candidate_set[max_index,:], ei[max_index]
        self.candidate_set = np.delete(self.candidate_set,(max_index),axis=0)
        return next_pt, next_ei

    def optimize(self, num_iters=100, path=None):
        for i in range(num_iters):
            print "Iteration " + str(i+1) + " of " + str(num_iters) + ":"
            self.model = GPy.models.GPRegression(self.X, self.y, self.kernel)
            self.model.optimize(messages=False)

            next_pt, ei_val = self.next_candidate_discrete(self.model)
            true_next_pt = self.unscale_point(next_pt)
            f_val = self.func(true_next_pt)
            
            self.X = np.concatenate((self.X, np.array([next_pt])))
            self.y = np.concatenate((self.y, np.array([[f_val]])))

            if f_val < self.best_y:
                self.best_x = true_next_pt
                self.best_y = f_val
            if path is not None:
                outfile = open(path,"w")
                np.savez(outfile, X=self.X, y=self.y, best_x=self.best_x, best_y=self.best_y)
            print "Objective value = %.3f, EI = %.3f, current best = %.3f" % (f_val,ei_val[0],self.best_y)
