#!/bin/sh

. /scripts/utils.sh

VALKEY_HOST=$1
SCRIPT_TIMEOUT=$2
start_time=$(date +%s)
end_time=$((start_time + SCRIPT_TIMEOUT))

msg meet begin

# Function to get pod IP from Kubernetes API
get_pod_ip() {
	local pod_name=$1
	local namespace=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
	local token=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
	local cacert=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt

	# Query Kubernetes API for the specific pod
	curl -s --cacert "$cacert" --header "Authorization: Bearer $token" \
		"https://kubernetes.default.svc/api/v1/namespaces/$namespace/pods/$pod_name" |
		grep -o '"podIP":"[^"]*"' | head -1 | cut -d'"' -f4
}

while [ "$(date +%s)" -lt "$end_time" ]; do
	sleep 1

	# Extract pod name from FQDN (take everything before first dot)
	POD_NAME=$(echo "$VALKEY_HOST" | cut -d'.' -f1)

	# Try to get IP from Kubernetes API first
	ipaddress=$(get_pod_ip "$POD_NAME")

	# Fall back to DNS if Kubernetes API fails
	if [ -z "$ipaddress" ]; then
		msg meet "Kubernetes API lookup failed, falling back to DNS for $VALKEY_HOST"
		ipaddress=$(getent hosts "$VALKEY_HOST" | awk '{ print $1 }')
	fi

	if [ -z "$ipaddress" ]; then
		msg meet "Could not resolve $VALKEY_HOST"
		continue
	fi

	msg meet "valkey_cli $ipaddress 6379 -t 1 -c ping"
	RESPONSE=$(valkey_cli "$ipaddress" 6379 -t 1 -c ping)

	if [ "$RESPONSE" = "PONG" ]; then
		msg meet "PONG response from $ipaddress ($VALKEY_HOST)"
		msg meet "MEET $ipaddress"
		valkey_cli 127.0.0.1 6379 -t 1 -c cluster meet "$ipaddress" 6379
		break
	else
		msg meet "got response from $ipaddress: ${RESPONSE}"
		continue
	fi
done

msg meet end
