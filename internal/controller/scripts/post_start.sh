#!/bin/sh

set -e

# use hostnames from cluster nodes to re-meet in the case that the ips have been updated
for host in $(valkey-cli -h 127.0.0.1 -p 6379 -c cluster nodes | awk '!/myself/ { split($2, arr, ","); print arr[2] }'); do
	ipaddress=$(LC_ALL=C nslookup "$host" 2>/dev/null | sed -nr '/Name/,+1s|Address(es)?: *||p')
	valkey-cli -h 127.0.0.1 -p 6379 -c cluster meet "$ipaddress" 6379
done
