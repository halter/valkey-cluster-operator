#!/bin/sh

# Ready when master, or a replica whose replication link is up and initial sync has finished.
# Does not use utils.sh valkey_cli: it logs every call to /data/logs.

if [ -s /scripts/password ]; then
	REDISCLI_AUTH="$(cat /scripts/password)"
	export REDISCLI_AUTH
fi

info="$(valkey-cli -h 127.0.0.1 -p 6379 --no-auth-warning info replication 2>&1 | tr -d '\r')"

field() {
	printf '%s\n' "$info" | awk -F: -v key="$1" '$1 == key { print $2; exit }'
}

role="$(field role)"
case "$role" in
master)
	exit 0
	;;
slave)
	link="$(field master_link_status)"
	sync="$(field master_sync_in_progress)"
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
