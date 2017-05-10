Installations Required:

Google Compute Platform SDK (https://cloud.google.com/sdk/)  
GCP API Client for Python (https://github.com/google/google-api-python-client)  
SciPy (https://www.scipy.org/)  
PySOT and POAP (https://github.com/dme65/pySOT)  
GPy (https://github.com/SheffieldML/GPy)
Scikit Learn (http://scikit-learn.org/stable/)


Folders:

cloud  
* Contains GCP Python API scripts for managing Surrogate Optimization Workers 

searchtree  
* Contains Decision Tree class
* Pruning, Searching, and related functions included here

tests  
* Contains unit tests, sample problems
* Currently contains Gaussian Process scripts as well

utility  
* Contains utility functions, including transition function construction (among others)
* Transition Function Contruction requires SciPy and SciKit-Learn

Tests to run: 

gp/test-noisysine.py tests the Surrogate Construction Process   
cloud/test-tcp.py tests basic communication between Master and Workers on a single machine
