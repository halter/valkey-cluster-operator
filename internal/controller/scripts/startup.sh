#!/bin/bash

# Valkey node startup check. The node is considered started when any of:
# 1. Cluster state is "ok"
# 2. This node doesn't know about any other nodes (fresh single node)
# 3. This node has no slots assigned (nothing to serve yet)
# 4. TIMEOUT_SECONDS of uptime have elapsed (fail open rather than blocking
#    the rollout forever; the startup probe's failureThreshold * periodSeconds
#    must stay above this so kubelet doesn't restart the container first)

# shellcheck source=./utils.sh
. /scripts/utils.sh

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"

if valkey_cli 127.0.0.1 6379 -t 1 -c CLUSTER INFO 2>/dev/null | grep -q "cluster_state:ok"; then
	echo "Startup check passed: cluster state is ok"
	exit 0
fi

nodes=$(valkey_cli 127.0.0.1 6379 -t 1 -c CLUSTER NODES 2>/dev/null)
if [ -n "$nodes" ]; then
	if [ "$(echo "$nodes" | grep -c .)" -eq 1 ]; then
		echo "Startup check passed: node doesn't know about any other nodes"
		exit 0
	fi

	# Slot assignments are the fields after the first 8 on this node's line
	myself=$(echo "$nodes" | grep myself)
	if [ -n "$myself" ] && [ "$(echo "$myself" | awk '{print NF}')" -le 8 ]; then
		echo "Startup check passed: node has no slots assigned"
		exit 0
	fi
fi

uptime=$(valkey_cli 127.0.0.1 6379 -t 1 -c INFO server 2>/dev/null | grep "^uptime_in_seconds:" | cut -d: -f2 | tr -d '\r')
if [ -n "$uptime" ] && [ "$uptime" -ge "$TIMEOUT_SECONDS" ]; then
	echo "Startup check passed: uptime ${uptime}s exceeds ${TIMEOUT_SECONDS}s timeout"
	exit 0
fi

echo "Startup check failed: cluster state not ok, node has slots and peers, uptime ${uptime:-unknown}s below ${TIMEOUT_SECONDS}s timeout"
exit 1
