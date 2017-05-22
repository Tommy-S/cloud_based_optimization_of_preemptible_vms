#!/bin/bash

# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START startup_script]

# Use the metadata server to get the configuration specified during
# instance creation. Read more about metadata here:
# https://cloud.google.com/compute/docs/metadata#querying
HOSTIP=$(curl http://metadata/computeMetadata/v1/instance/attributes/hostip -H "Metadata-Flavor: Google")
PORT=$(curl http://metadata/computeMetadata/v1/instance/attributes/port -H "Metadata-Flavor: Google")

mkdir playground
cd playground

touch runfile.py
echo 'from poapextensions.GCPVirtualMachine import GCPWorkerManager' >> runfile.py
echo 'import sys' >> runfile.py
echo 'hostip = sys.argv[1]' >> runfile.py
echo 'port = int(sys.argv[2])' >> runfile.py
echo 'GCPWorkerManager(hostip, port, retries=1).run()' >> runfile.py

# python runfile.py $HOSTIP $PORT

# Create a Google Cloud Storage bucket.
# gsutil mb gs://$CS_BUCKET

# Store the image in the Google Cloud Storage bucket and allow all users
# to read it.
# gsutil cp -a public-read testfile.txt gs://$CS_BUCKET/testfile.txt

# [END startup_script]