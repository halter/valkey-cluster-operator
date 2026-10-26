#!/bin/sh

msg() {
	CONTEXT=$1
	MSG=$2
	mkdir -p /data/logs
	echo "$(date --rfc-3339=seconds): ${CONTEXT} - ${MSG}" >>"/data/logs/${CONTEXT}.log"
}

# valkey_cli HOST PORT ARGS...: run valkey-cli against HOST:PORT, authenticating
# when the cluster has a password. The password goes through REDISCLI_AUTH,
# which every valkey-cli since 8.0 reads (9.x prefers VALKEYCLI_AUTH and falls
# back to it), rather than -a: -a shows the password in the process list and
# this function used to write it into /data/logs/valkey_cli.log on every call.
valkey_cli() {
	msg valkey_cli begin

	IP=$1
	PORT=$2
	shift 2

	if [ -s /scripts/password ]; then
		REDISCLI_AUTH="$(cat /scripts/password)"
		export REDISCLI_AUTH
	fi

	msg valkey_cli "valkey-cli -h $IP -p $PORT $*"
	valkey-cli -h "$IP" -p "$PORT" --no-auth-warning "$@"
	msg valkey_cli end
}
