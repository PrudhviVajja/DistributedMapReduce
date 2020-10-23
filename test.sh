!/bin/bash

# gcloud compute --project prudhvi-vajja firewall-rules create vpnrulemaster --allow tcp:8080 --
gcloud compute firewall-rules create MY-RULE --allow tcp
sudo apt-get -y update
sudo apt-get -y install git

# Install Requirments
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python3 get-pip.py
pip3 install --upgrade google-api-python-client
pip3 install oauth2client
pip3 install rpyc


sudo mkdir /usr/mapreduce
sudo chmod 777 /usr/mapreduce
cd /usr/mapreduce

# Git Clone
rm -rf DistributedMapreduce
sudo git clone https://github.com/PrudhviVajja/DistributedMapReduce.git

ls
echo $(ls)
echo "Cloning is Done..."
# chmod 777 /home/Distributed-Mapreduce/master.py
# chmod 777 /home/Distributed-Mapreduce/gcp.py

# Run the Master program in background
# sudo chmod 777 /Distributed-Mapreduce
cd /usr/mapreduce/DistributedMapreduce
echo $(ls)
ls
sudo python3 DistributedMapreduce/test.py 