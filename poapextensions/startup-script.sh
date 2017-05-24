#!/bin/bash

# This script is run on creation of a Debian Google Compute Platform server.
# It sets installs necessary tools, gets communication information for its
# monitor, and starts up a GCEWorkerManager.

# [START REQUIRED SERVER CONFIG]
sudo apt-get update
sudo apt-get install -y git-core
sudo apt-get install -y python-dev build-essential
sudo apt-get install -y python-pip

mkdir /playground
cd /playground

git clone https://github.com/prw54/cloud_based_optimization_of_preemptible_vms.git
cd cloud_based_optimization_of_preemptible_vms

# Install the poap extensions
sudo pip install -r requirements.txt
sudo pip install -e .

cd /playground

HOSTIP=$(curl http://metadata/computeMetadata/v1/instance/attributes/hostip -H "Metadata-Flavor: Google")
PORT=$(curl http://metadata/computeMetadata/v1/instance/attributes/port -H "Metadata-Flavor: Google")

touch runfile.py
echo 'import logging' >> runfile.py
echo 'logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)' >> runfile.py

echo 'import sys' >> runfile.py
echo 'hostip = sys.argv[1]' >> runfile.py
echo 'port = int(sys.argv[2])' >> runfile.py

echo 'from poapextensions.GCEVirtualMachine import GCEWorkerManager' >> runfile.py
# [END REQUIRED SERVER CONFIG]

# A function to launch local workers may be provided as the 
# launchWorker argument. An example can be found in GCEVirtualMachine.py
echo 'GCEWorkerManager(hostip, port, retries=1).run()' >> runfile.py

# Start the GCEWorkerManager
python runfile.py $HOSTIP $PORT