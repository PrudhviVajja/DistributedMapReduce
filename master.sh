#!/bin/bash

# gcloud compute --project prudhvi-vajja firewall-rules create vpnrulemaster --allow tcp:8080
gcloud compute firewall-rules create MY-RULE --allow tcp
sudo apt-get -y update
sudo apt-get -y install git

# Install Requirments
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python3 get-pip.py
pip3 install --upgrade google-api-python-client
pip3 install oauth2client
pip3 install rpyc


sudo mkdir /usr
sudo chmod 777 /usr
cd /usr

# Git Clone
rm -rf Distributed-Mapreduce
sudo git clone https://github.com/PrudhviVajja/Distribute-MapReduce.git

ls
echo $(ls)
echo "Cloning is Done..."
# chmod 777 /home/Distributed-Mapreduce/master.py
# chmod 777 /home/Distributed-Mapreduce/gcp.py

# Run the Master program in background
# sudo chmod 777 /Distributed-Mapreduce
cd Distributed-Mapreduce
echo $(ls)
ls
sudo python3 master.py