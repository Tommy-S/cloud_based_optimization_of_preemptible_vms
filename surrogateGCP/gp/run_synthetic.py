import numpy as np
from bayesopt import *

def stybtang(x):
    if len(x.shape) == 1:
        return 0.5 * np.sum(np.power(x,4) - 16 * np.power(x,2) + 5*x)
    else:
        return 0.5 * np.sum(np.power(x,4) - 16 * np.power(x,2) + 5*x,axis=1) 

num_iters = 30

func = stybtang               # Function handle to optimize (can be a handle that binds constant arguments)
num_dims = 3                  # Number of dimensions of f
mins = -3*np.ones(num_dims)   # Minimum value for each parameter to search over (d-by-1)
maxes = 3*np.ones(num_dims)   # Maximum value for each parameter to search over (d-by-1)
grid_size = 10000             # Size of grid to use for discrete EI optimization
kernel = GPy.kern.RBF(input_dim=num_dims,active_dims=range(num_dims))
bo_model = FixedModelBayesOpt(func, kernel, num_dims, grid_size, mins, maxes)
bo_model.initialize_run(num_iters=3)  # Runs num_iters points at random first (must be at least 1) so we have something to train on
bo_model.optimize(num_iters)

best_x = bo_model.best_x
best_y = bo_model.best_y
print "Best x, ", best_x
print "f(x) = ", best_y

