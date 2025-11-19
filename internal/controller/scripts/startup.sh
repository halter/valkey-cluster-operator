#!/bin/bash

# Valkey Node Startup Check for Kubernetes
# Returns 0 (started) if any of these conditions are met:
# 1. Cluster state is "ok"
# 2. Node has zero slots allocated
# 3. Node doesn't know about any other nodes (single node)
# 4. 300 seconds have elapsed since pod started

set -x

# shellcheck source=./utils.sh
. /scripts/utils.sh

# Configuration
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"

# Function to check if timeout has elapsed since Valkey started
check_timeout() {
	local info_output
	info_output=$(valkey_cli 127.0.0.1 6379 -t 1 -c INFO server 2>/dev/null || echo "")

	# Extract uptime_in_seconds from INFO output
	local uptime
	uptime=$(echo "$info_output" | grep "^uptime_in_seconds:" | cut -d: -f2 | tr -d '\r')

	if [ -z "$uptime" ]; then
		echo "Warning: Could not retrieve Valkey uptime" >&2
		return 1
	fi

	if [ "$uptime" -ge $TIMEOUT_SECONDS ]; then
		echo "Startup check passed: Valkey uptime of ${uptime}s exceeds timeout of ${TIMEOUT_SECONDS}s"
		return 0
	fi
	return 1
}

# Function to check cluster state
check_cluster_state() {
	local cluster_info
	cluster_info=$(valkey_cli 127.0.0.1 6379 -t 1 -c CLUSTER INFO 2>/dev/null || echo "")

	if echo "$cluster_info" | grep -q "cluster_state:ok"; then
		echo "Startup check passed: cluster state is ok"
		return 0
	fi
	return 1
}

# Function to check slot allocation
check_slots() {
	local nodes_info
	nodes_info=$(valkey_cli 127.0.0.1 6379 -t 1 -c CLUSTER NODES 2>/dev/null || echo "")

	# Find the current node (marked with "myself")
	local myself_line
	myself_line=$(echo "$nodes_info" | grep "myself" || echo "")

	if [ -z "$myself_line" ]; then
		echo "Warning: Could not find current node in cluster nodes output" >&2
		return 1
	fi

	# Check if the line contains any slot ranges (format: [slot-slot] or single slots)
	# Slots appear after the address and flags, typically after the 8th field
	if ! echo "$myself_line" | grep -qE '\[?[0-9]+-?[0-9]*\]?'; then
		echo "Startup check passed: node has zero slots allocated"
		return 0
	fi

	return 1
}

# Function to check if node doesn't know about any other nodes
check_single_node() {
	local nodes_info
	nodes_info=$(valkey_cli 127.0.0.1 6379 -t 1 -c CLUSTER NODES 2>/dev/null || echo "")

	if [ -z "$nodes_info" ]; then
		echo "Warning: Could not retrieve cluster nodes information" >&2
		return 1
	fi

	# Count the number of nodes (each node is one line)
	local node_count
	node_count=$(echo "$nodes_info" | grep -c "^")

	if [ "$node_count" -eq 1 ]; then
		echo "Startup check passed: node doesn't know about any other nodes"
		return 0
	fi

	return 1
}

# Main startup check logic
main() {
	# Check condition 4: timeout elapsed
	if check_timeout; then
		exit 0
	fi

	# Check condition 1: cluster state ok
	if check_cluster_state; then
		exit 0
	fi

	# Check condition 3: single node (doesn't know about other nodes)
	if check_single_node; then
		exit 0
	fi

	# Check condition 2: zero slots allocated
	if check_slots; then
		exit 0
	fi

	# None of the conditions met
	echo "Startup check failed: waiting for cluster state ok, zero slots, single node, or timeout"
	exit 1
}

main
