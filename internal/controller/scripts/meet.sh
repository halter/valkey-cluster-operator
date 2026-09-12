#!/bin/sh

. /scripts/utils.sh

VALKEY_HOST=$1
SCRIPT_TIMEOUT=${2:-${MEET_TIMEOUT_SECONDS:-600}}
start_time=$(date +%s)
end_time=$((start_time + SCRIPT_TIMEOUT))

msg meet "begin $VALKEY_HOST timeout ${SCRIPT_TIMEOUT}s"

while [ "$(date +%s)" -lt "$end_time" ]; do
	sleep 1
	RESPONSE=$(valkey_cli "$VALKEY_HOST" 6379 -t 1 -c ping)
	if [ "$RESPONSE" != "PONG" ]; then
		msg meet "got response from $VALKEY_HOST: ${RESPONSE}"
		continue
	fi

	ipaddress=$(getent hosts "$VALKEY_HOST" | awk '{ print $1; exit }')
	if [ -z "$ipaddress" ]; then
		msg meet "could not resolve $VALKEY_HOST"
		continue
	fi

	RESPONSE=$(valkey_cli 127.0.0.1 6379 -t 1 -c cluster meet "$ipaddress" 6379)
	if [ "$RESPONSE" = "OK" ]; then
		msg meet "MEET $ipaddress OK"
		break
	fi
	msg meet "MEET $ipaddress failed: ${RESPONSE}"
done

msg meet end
