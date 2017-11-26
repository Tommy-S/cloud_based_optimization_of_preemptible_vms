from threading import Thread, Lock, Condition, Semaphore
import socket
import sys
import boto3
from botocore.exceptions import ClientError

ec2 = boto3.client('ec2')


#create vm
#UserData contains startup script that is run by VM upon startup
#VM will make TCP connection to server upon startup
vm = ec2.run_instances(
    ImageId='ami-8c1be5f6',
    InstanceType='t2.micro',
    KeyName='poaptest',
    MaxCount=1,
    MinCount=1,
    UserData= """
                    #!/bin/bash

                    sudo yum upgrade -y
                    sudo yum install -y git 
                    pip install boto3


                    touch runfile.py
                    echo 'import logging' >> runfile.py
                    echo 'logging.basicConfig(format="%(name)-18s: %(levelname)-8s %(message)s", level=logging.DEBUG)' >> runfile.py

                    echo 'import sys' >> runfile.py
                    echo 'import socket' >> runfile.py
                    echo 'hostip = "54.205.26.158" ' >> runfile.py
                    echo 'sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)' >> runfile.py
                    echo 'sock.connect((hostip,44100))' >> runfile.py
                    echo 'sock.send("1")' >> runfile.py

                    python runfile.py 
            """

    
)

#initiate server
hostip = '127.0.0.1'
socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
socket.bind(('', 44100))
print('enter')

socket.listen(1000000000)
handler,addr = socket.accept()
print ('connected to' + str(addr))
message_from_client = (handler.recv(4096))
print(message_from_client)
ec2.terminate_instances(InstanceIds=[vm['Instances'][0]['InstanceId']])
print('done')
