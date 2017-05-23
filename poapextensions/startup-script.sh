#!/bin/bash

HOSTIP=$(curl http://metadata/computeMetadata/v1/instance/attributes/hostip -H "Metadata-Flavor: Google")
PORT=$(curl http://metadata/computeMetadata/v1/instance/attributes/port -H "Metadata-Flavor: Google")

mkdir /playground
cd /playground

touch runfile.py
echo 'import logging' >> runfile.py
echo 'logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)' >> runfile.py

echo 'import sys' >> runfile.py
echo 'hostip = sys.argv[1]' >> runfile.py
echo 'port = int(sys.argv[2])' >> runfile.py

echo 'from poapextensions.GCPVirtualMachine import GCPWorkerManager' >> runfile.py
echo 'GCPWorkerManager(hostip, port, retries=1).run()' >> runfile.py

python runfile.py $HOSTIP $PORT