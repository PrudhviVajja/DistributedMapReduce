#!/bin/bash

gcloud compute --project prudhvi-vajja firewall-rules create vpnrulemaster --allow tcp:8080
sudo apt-get -y update
sudo apt-get -y install git

# Git Clone
rm -rf Distributed-Mapreduce
git clone https://github.com/PrudhviVajja/Distribute-MapReduce.git

# chmod 777 /home/Distributed-Mapreduce/master.py
# chmod 777 /home/Distributed-Mapreduce/gcp.py

# Install Requirments
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python3 get-pip.py
pip3 install --upgrade google-api-python-client
pip3 install oauth2client
pip3 install rpyc

# Run the Master program in background
cd Distributed-Mapreduce
python3 master.py &