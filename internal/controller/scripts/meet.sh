#!/bin/sh

. /scripts/utils.sh

VALKEY_HOST=$1
SCRIPT_TIMEOUT=$2
start_time=$(date +%s)
end_time=$((start_time + SCRIPT_TIMEOUT))

msg meet begin

while [ "$(date +%s)" -lt "$end_time" ]; do
	sleep 1
	msg meet "valkey_cli $VALKEY_HOST 6379 -t 1 -c ping"
	RESPONSE=$(valkey_cli "$VALKEY_HOST" 6379 -t 1 -c ping)

	if [ "$RESPONSE" = "PONG" ]; then
		msg meet "PONG response from $VALKEY_HOST"
		msg meet "MEET $VALKEY_HOST"
		valkey_cli 127.0.0.1 6379 -t 1 -c cluster meet "$VALKEY_HOST" 6379
		break
	else
		msg meet "got response from $VALKEY_HOST: ${RESPONSE}"
		continue
	fi
done

msg meet end
