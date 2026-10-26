#!/bin/sh

# Ready when the dataset has finished loading and the node is a master, or a
# replica whose replication link is up and initial sync has finished.
# Does not use utils.sh valkey_cli: it logs every call to /data/logs.

if [ -s /scripts/password ]; then
	REDISCLI_AUTH="$(cat /scripts/password)"
	export REDISCLI_AUTH
fi

cli() {
	valkey-cli -h 127.0.0.1 -p 6379 --no-auth-warning "$@" 2>&1 | tr -d '\r'
}

# field KEY TEXT: the value of "KEY:value" in an INFO response
field() {
	printf '%s\n' "$2" | awk -F: -v key="$1" '$1 == key { print $2; exit }'
}

# INFO answers while the server is still loading its append-only file or a
# full-sync payload, so check that first for both roles: a loading master would
# otherwise be Ready and in Service endpoints before it can serve its keys.
persistence="$(cli info persistence)"
loading="$(field loading "$persistence")"
if [ "$loading" != "0" ]; then
	echo "not ready: loading=${loading:-unknown} ($(printf '%s\n' "$persistence" | head -n 1))"
	exit 1
fi

info="$(cli info replication)"
role="$(field role "$info")"
case "$role" in
master)
	exit 0
	;;
slave)
	link="$(field master_link_status "$info")"
	sync="$(field master_sync_in_progress "$info")"
	if [ "$link" = "up" ] && [ "$sync" = "0" ]; then
		exit 0
	fi
	echo "not ready: replica master_link_status=${link:-unknown} master_sync_in_progress=${sync:-unknown}"
	exit 1
	;;
*)
	echo "not ready: $(printf '%s\n' "$info" | head -n 1)"
	exit 1
	;;
esac
