#!/bin/sh

msg() {
	CONTEXT=$1
	MSG=$2
	mkdir -p /data/logs
	echo "$(date --rfc-3339=seconds): ${CONTEXT} - ${MSG}" >>"/data/logs/${CONTEXT}.log"
}

valkey_cli() {
	msg valkey_cli begin

	IP=$1
	PORT=$2
	shift 2
	ARGS=$@

	msg valkey_cli "valkey-cli -h $IP -p $PORT info"
	result=$(valkey-cli -h "$IP" -p "$PORT" info)
	msg valkey_cli "result: $result"

	if echo "$result" | grep -q "NOAUTH"; then
		msg valkey_cli "AUTH_REQUIRED=true"
		AUTH_REQUIRED=true
	fi

	if [ "$AUTH_REQUIRED" = "true" ]; then
		msg valkey_cli "valkey-cli -h $IP -p $PORT -a $(cat /scripts/password) info"
		result=$(valkey-cli -h "$IP" -p "$PORT" -a "$(cat /scripts/password)" info)
		msg valkey_cli "result: $result"
		if echo "$result" | grep -q "ERR invalid password"; then
			msg valkey_cli end
			exit 0
		fi
	fi

	VALKEY_CMD="valkey-cli -h ${IP} -p ${PORT}"
	if [ "$AUTH_REQUIRED" = "true" ]; then
		VALKEY_CMD="$VALKEY_CMD -a $(cat /scripts/password)"
	fi

	msg valkey_cli "$VALKEY_CMD $ARGS"
	$VALKEY_CMD $ARGS
	msg valkey_cli end
}
