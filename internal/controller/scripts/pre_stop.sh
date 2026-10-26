#!/bin/sh

. /scripts/utils.sh

# the kubelet discards hook output; mirror to the container's stdout
surface() {
	msg pre_stop "$1"
	echo "pre-stop: $1" >/proc/1/fd/1 2>/dev/null || true
}

# keep the whole hook within the pod's 30s termination grace period
FAILOVER_WAIT_SECONDS=10
DRAIN_SLEEP_SECONDS=10

VALKEY_ROLE="$(valkey_cli 127.0.0.1 6379 -c info replication 2>/dev/null | awk '/^role:/ { print $1 }' | tr -d '\r')"

if [ "${VALKEY_ROLE}" != "role:master" ]; then
	surface "skipping failover: role is '${VALKEY_ROLE:-unknown}', not master"
	exit 0
fi

CONNECTED_REPLICAS="$(valkey_cli 127.0.0.1 6379 -c info replication | awk -F: '/^connected_slaves:/ { print $2 }' | tr -d '\r')"

if [ "${CONNECTED_REPLICAS:-0}" -le "0" ]; then
	surface "skipping failover: no connected replicas; slots served by this node are unavailable until it restarts"
	exit 0
fi

# Pick the online replica with the highest replication offset. A replica still
# receiving its initial sync reports state=wait_bgsave or send_bulk and cannot
# be promoted, so looking only at the first listed replica would skip the
# failover whenever that one happens to be the one mid-sync.
REPLICA="$(valkey_cli 127.0.0.1 6379 -c info replication | tr -d '\r' | awk -F: '/^slave[0-9]+:/ { print $2 }' | tr , ' ' | awk '
	{
		ip = ""; port = ""; state = ""; offset = -1
		for (i = 1; i <= NF; i++) {
			split($i, kv, "=")
			if (kv[1] == "ip") ip = kv[2]
			if (kv[1] == "port") port = kv[2]
			if (kv[1] == "state") state = kv[2]
			if (kv[1] == "offset") offset = kv[2] + 0
		}
		if (state == "online" && ip != "" && port != "" && offset > best) {
			best = offset; pick = ip " " port
		}
	}
	BEGIN { best = -1; pick = "" }
	END { print pick }')"

if [ -z "${REPLICA}" ]; then
	surface "skipping failover: none of the ${CONNECTED_REPLICAS} connected replicas is online yet"
	exit 0
fi
SLAVE_IP="${REPLICA% *}"
SLAVE_PORT="${REPLICA#* }"

surface "initiating failover to replica ${SLAVE_IP}:${SLAVE_PORT}"
valkey_cli "$SLAVE_IP" "$SLAVE_PORT" -c cluster failover

check_failover_complete() {
	ROLE=$(valkey_cli "$SLAVE_IP" "$SLAVE_PORT" INFO REPLICATION | grep role | cut -d':' -f2 | tr -d '\r')
	if [ "${ROLE}" = "master" ]; then
		return 0
	else
		return 1
	fi
}

WAITED=0
while ! check_failover_complete; do
	if [ "$WAITED" -ge "$FAILOVER_WAIT_SECONDS" ]; then
		surface "failover to ${SLAVE_IP}:${SLAVE_PORT} did not complete within ${FAILOVER_WAIT_SECONDS}s; shutting down anyway"
		exit 0
	fi
	echo "Failover in progress, waiting..."
	sleep 1
	WAITED=$((WAITED + 1))
done
surface "failover to ${SLAVE_IP}:${SLAVE_PORT} complete; draining for ${DRAIN_SLEEP_SECONDS}s"

sleep "$DRAIN_SLEEP_SECONDS"
