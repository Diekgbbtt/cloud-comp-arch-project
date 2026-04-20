#!/bin/bash

# Script to install and run augmented mcperf on client machines
# Usage: ./install_and_run_mcperf.sh <agent-a-ip> <agent-b-ip> <client-measure-ip>

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check argument count
if [ $# -ne 3 ]; then
    echo -e "${RED}Error: Expected 3 arguments (agent-a-ip, agent-b-ip, client-measure-ip)${NC}"
    echo "Usage: $0 <agent-a-ip> <agent-b-ip> <client-measure-ip>"
    exit 1
fi

AGENT_A_IP=$1
AGENT_B_IP=$2
CLIENT_MEASURE_IP=$3

echo -e "${YELLOW}========================================${NC}"
echo "Installing mcperf on cluster machines"
echo -e "${YELLOW}========================================${NC}"
echo "Agent A IP: $AGENT_A_IP"
echo "Agent B IP: $AGENT_B_IP"
echo "Client Measure IP: $CLIENT_MEASURE_IP"
echo ""

# Function to run installation on a remote machine
install_mcperf() {
    local ip=$1
    local machine_name=$2

    echo -e "${YELLOW}[$(date '+%H:%M:%S')] Installing mcperf on $machine_name ($ip)...${NC}"

    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "ubuntu@$ip" << 'EOF'
        set -e
        echo "Starting mcperf installation..."

        # Update apt sources
        echo "Updating apt sources..."
        sudo sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources

        # Update package lists
        echo "Running apt-get update..."
        sudo apt-get update

        # Install dependencies
        echo "Installing dependencies..."
        sudo apt-get install libevent-dev libzmq3-dev git make g++ --yes

        # Install build dependencies for memcached
        echo "Installing memcached build dependencies..."
        sudo apt-get build-dep memcached --yes

        # Clone and build mcperf
        echo "Cloning memcache-perf-dynamic repository..."
        if [ ! -d "memcache-perf-dynamic" ]; then
            git clone https://github.com/eth-easl/memcache-perf-dynamic.git
        fi

        cd memcache-perf-dynamic
        echo "Building mcperf..."
        make

        echo "mcperf installation completed successfully!"
EOF

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully installed mcperf on $machine_name${NC}"
    else
        echo -e "${RED}✗ Failed to install mcperf on $machine_name${NC}"
        exit 1
    fi
}

# Install on all three machines
install_mcperf "$AGENT_A_IP" "client-agent-a"
install_mcperf "$AGENT_B_IP" "client-agent-b"
install_mcperf "$CLIENT_MEASURE_IP" "client-measure"

echo ""
echo -e "${YELLOW}========================================${NC}"
echo "Starting mcperf load generation"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Run mcperf on agent-a
echo -e "${YELLOW}[$(date '+%H:%M:%S')] Starting mcperf on client-agent-a with -T 2 -A...${NC}"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "ubuntu@$AGENT_A_IP" << 'EOF' &
    cd memcache-perf-dynamic
    ./mcperf -T 2 -A
EOF
AGENT_A_PID=$!

# Run mcperf on agent-b
echo -e "${YELLOW}[$(date '+%H:%M:%S')] Starting mcperf on client-agent-b with -T 4 -A...${NC}"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "ubuntu@$AGENT_B_IP" << 'EOF' &
    cd memcache-perf-dynamic
    ./mcperf -T 4 -A
EOF
AGENT_B_PID=$!

echo ""
echo -e "${GREEN}✓ mcperf load generation started${NC}"
echo -e "${YELLOW}Agent A process PID: $AGENT_A_PID${NC}"
echo -e "${YELLOW}Agent B process PID: $AGENT_B_PID${NC}"
echo ""
echo "To stop the load generation, run:"
echo "  kill $AGENT_A_PID $AGENT_B_PID"
echo ""

# Wait for both processes (optional - comment out to let them run in background)
# wait $AGENT_A_PID $AGENT_B_PID
