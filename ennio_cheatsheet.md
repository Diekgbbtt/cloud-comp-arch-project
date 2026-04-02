# CCA Project Cheatsheet

## Part 1

### Initialization

gsutil mb gs://cca-eth-2026-group-059-efattiboni/

$env:KOPS_STATE_STORE = "gs://cca-eth-2026-group-059-efattiboni/"
$env:PROJECT = "gcloud config get-value project"

$env:KOPS_STATE_STORE
$env:PROJECT

### Deploying cluster

cd C:\Users\ennio\Documents\MastersLocal\CCA\cloud-comp-arch-project

kops create -f part1.yaml

kops create secret --name part1.k8s.local sshpublickey admin -i "C:\Users\ennio\.ssh\cloud-computing.pub"

kops update cluster --name part1.k8s.local --yes --admin

kops validate cluster --wait 10m

kubectl get nodes -o wide

gcloud compute ssh --ssh-key-file "C:\Users\ennio\.ssh\cloud-computing" ubuntu@client-agent-zrwv --zone europe-west1-b

ssh -i "C:\Users\ennio\.ssh\cloud-computing" ubuntu@35.205.150.214

### Running memcached and the mcperf load generator

_This line helped me solve my error_
kubectl label node memcache-server-kh5g cca-project-nodetype=memcached

kubectl create -f memcache-t1-cpuset.yaml
kubectl expose pod some-memcached --name some-memcached-11211 `
--type LoadBalancer --port 11211 `
--protocol TCP
sleep 60
kubectl2 get service some-memcached-11211

kubectl get pods -o wide

_SSH into client-agent and client-measure and run_
sudo apt-get update
sudo apt-get install libevent-dev libzmq3-dev git make g++ --yes
sudo sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources
sudo apt-get update
sudo apt-get build-dep memcached --yes
cd && git clone https://github.com/shaygalon/memcache-perf.git
cd memcache-perf
git checkout 0afbe9b
make

_On client-agent_
./mcperf -T 8 -A

_On client measure, replacing MEMCACHED from kubectl get pods -o wide and INTERNAL AGENT IP from kubectl get nodes -o wide_
./mcperf -s MEMCACHED_IP --loadonly
./mcperf -s MEMCACHED_IP -a INTERNAL_AGENT_IP \
--noload -T 8 -C 8 -D 4 -Q 1000 -c 8 -t 5 -w 2 \
--scan 5000:80000:5000

### Introducing Resource Interference

kubectl create -f interference/ibench-cpu.yaml

kubectl get pods -o wide

_Command found online to enter the pod_
kubectl exec -it ibench-cpu -- /bin/bash

kubectl delete pods ibench-cpu

### IMPORTANT DELETE YOUR CLUSTER

kops delete cluster part1.k8s.local --yes

## Part 2
