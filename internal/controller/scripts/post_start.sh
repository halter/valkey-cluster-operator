#!/bin/sh

set -e

# use hostnames from cluster nodes to re-meet in the case that the ips have
# been updated. The hostname has been set via the nodename field as setting a
# node hostname to a coreDNS record causes some trouble for clients that don't
# respect cluster-preferred-endpoint-type when connection from outside kubernetes

# run cluster meet operation 3 times on start up to give time for other pods to start
for i in {1..3}; do
	for host in $(awk -F, '/nodename=/ && !/myself/ { split($5, arr, "="); print arr[2] }' nodes.conf); do
		ipaddress=$(LC_ALL=C nslookup "$host" 2>/dev/null | sed -nr '/Name/,+1s|Address(es)?: *||p')
		valkey-cli -h 127.0.0.1 -p 6379 -c cluster meet "$ipaddress" 6379
	done
	sleep 15
done
