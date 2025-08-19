#!/bin/sh

. /scripts/utils.sh

SCRIPT_TIMEOUT=10
start_time=$(date +%s)
end_time=$((start_time + SCRIPT_TIMEOUT))

# use hostnames from cluster nodes to re-meet in the case that the ips have
# been updated. The hostname has been set via the nodename field as setting a
# node hostname to a coreDNS record causes some trouble for clients that don't
# respect cluster-preferred-endpoint-type when connection from outside kubernetes

msg post_start begin

awk -F, '/nodename=/ && !/myself/ { split($5, arr, "="); print arr[2] }' nodes.conf | while IFS= read -r host; do
	(
		while [ "$(date +%s)" -lt "$end_time" ]; do

			msg post_start "valkey_cli $host 6379 -t 1 -c ping"
			RESPONSE=$(valkey_cli "$host" 6379 -t 1 -c ping)

			if [ "$RESPONSE" = "PONG" ]; then
				msg post_start "PONG response from $host"

				ipaddress=$(getent hosts "$host" | awk '{ print $1 }')

				msg post_start "MEET $ipaddress"

				valkey_cli 127.0.0.1 6379 -t 1 -c cluster meet "$ipaddress" 6379
				break
			else
				msg post_start "got response from $host: ${RESPONSE}"
				sleep 1
				continue
			fi
		done

	) &
done

wait

msg post_start end
