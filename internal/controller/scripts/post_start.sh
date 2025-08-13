#!/bin/sh

# use hostnames from cluster nodes to re-meet in the case that the ips have
# been updated. The hostname has been set via the nodename field as setting a
# node hostname to a coreDNS record causes some trouble for clients that don't
# respect cluster-preferred-endpoint-type when connection from outside kubernetes

mkdir -p /data/logs

echo "$(date --rfc-3339=seconds): post_start.sh begin" >>/data/logs/post_start.log

awk -F, '/nodename=/ && !/myself/ { split($5, arr, "="); print arr[2] }' nodes.conf | while IFS= read -r host; do
	(
		max_retries=100
		retry_count=0
		while [ "$retry_count" -lt "$max_retries" ]; do
			retry_count++
			RESPONSE=$(valkey-cli -h "$host" -p 6379 -c ping)
			if [ "$RESPONSE" = "PONG" ]; then
				echo "$(date --rfc-3339=seconds): PONG response from $host" >>/data/logs/post_start.log
				ipaddress=$(LC_ALL=C nslookup "$host" 2>/dev/null | sed -nr '/Name/,+1s|Address(es)?: *||p')
				echo "$(date --rfc-3339=seconds): MEET $ipaddress" >>/data/logs/post_start.log
				valkey-cli -h 127.0.0.1 -p 6379 -c cluster meet "$ipaddress" 6379
				break
			else
				retry_count=$((retry_count + 1))
				sleep 1
				continue
			fi
		done

	) &
done

wait

echo "$(date --rfc-3339=seconds): post_start.sh end" >>/data/logs/post_start.log
