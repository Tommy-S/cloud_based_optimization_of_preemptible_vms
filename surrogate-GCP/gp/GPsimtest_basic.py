import GPsim
import numpy as np

def f(x):
	return np.sin(x)

def main():
	bounds = [-3., 3]
	test = GPsim.GPsim(batchsize=100)
	test.run(f, bounds)

if __name__ == '__main__':
	main()