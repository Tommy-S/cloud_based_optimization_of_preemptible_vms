#!/bin/bash

# for host:
touch startup-script.sh
echo "echo 'startup script executed.'" >> startup-script.sh
export IP=$(curl http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip --header "Metadata-Flavor: Google")
sed -i '1i\'"export IP=$IP" startup-script.sh

export HOSTNAME=$(curl http://metadata.google.internal/computeMetadata/v1/instance/hostname --header "Metadata-Flavor: Google")
sed -i '1i\'"export IP=$IP" startup-script.sh

# [START startup_script]

# [END startup_script]
