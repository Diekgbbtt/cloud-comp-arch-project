*Create cluster*
kops create -f part4.yaml
kops update cluster --name part4.k8s.local --yes --admin
kops validate cluster --wait 10m

kubectl get nodes -o wide

*SSH to memcache-server*
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing2 ubuntu@memcache-server-w42x \
--zone europe-west1-b --tunnel-through-iap

ssh -i ~/.ssh/cloud-computing ubuntu@35.159.97.227

*Install memcached*
sudo apt update
sudo apt install -y memcached libmemcached-tools
sudo systemctl status memcached

*Increase memory limit*
sudo nano /etc/memcached.conf
*Update line -m with 1024 and lines with -l with INTERNAL_IP of memcache-server*
*Update threads by adding line -t*
sudo systemctl restart memcached
*Set taskaffinity of memcached*
sudo taskset -a -cp 0 4125
*Send files through scp*
gcloud compute scp --ssh-key-file ~/.ssh/cloud-computing2 ~/CCA/cloud-comp-arch-project/part4/scheduler.py ubuntu@memcache-server-jzkb:~ --tunnel-through-iap --zone=europe-west1-b
*Install docker https://docs.docker.com/engine/install/ubuntu/*
sudo usermod -a -G docker ubuntu
newgrp docker
*Create venv*
python3 -m venv ./venv
source ./venv/bin/activate
python3 -m pip install docker psutil
*Send files back*
gcloud compute scp --ssh-key-file ~/.ssh/cloud-computing2 ubuntu@memcache-server-jzkb:~/log20260510_023434.txt ~/CCA/cloud-comp-arch-project/part4/results/4_2/  --tunnel-through-iap --zone=europe-west1-b

*SSH to client machines and install*
sudo sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources
sudo apt-get update
sudo apt-get install libevent-dev libzmq3-dev git make g++ --yes
sudo apt-get build-dep memcached --yes
git clone https://github.com/eth-easl/memcache-perf-dynamic.git
cd memcache-perf-dynamic
make

*On client-agent*
./mcperf -T 8 -A

*On client-measure*
./mcperf -s INTERNAL_MEMCACHED_IP --loadonly
./mcperf -s INTERNAL_MEMCACHED_IP -a INTERNAL_AGENT_IP \
--noload -T 8 -C 8 -D 4 -Q 1000 -c 8 -t 1800 \
--qps_interval 15 --qps_min 5000 --qps_max 110000

*Part 4.1 record CPU utilization*
for i in {1..70}; do echo -n "$(date +%s%3N),"; mpstat -P 0 1 1 | awk '$2 ~ /[0-9]+/ && $1 == "Average:" {sum += 100 - $12} END {printf "%.1f\n", sum}'; sleep 0.1; done > cpu_core_usage.txt

for i in {1..60}; do echo -n "$(date +%s%3N),"; mpstat -P 0-2 1 1 | awk '$2 ~ /[0-9]+/ && $1 == "Average:" {sum += 100 - $12} END {printf "%.1f\n", sum}'; sleep 0.1; done > cpu_core_usage.txt
