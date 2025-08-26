#!/bin/sh

. /scripts/utils.sh

# use hostnames from cluster nodes to re-meet in the case that the ips have
# been updated. The hostname has been set via the nodename field as setting a
# node hostname to a coreDNS record causes some trouble for clients that don't
# respect cluster-preferred-endpoint-type when connection from outside kubernetes

msg post_start begin

awk -F, '/nodename=/ && !/myself/ { split($5, arr, "="); print arr[2] }' nodes.conf | while IFS= read -r host; do
	sh /scripts/meet.sh "$host" "$(shuf -i 5-30 -n1)" &
done

wait

msg post_start end
