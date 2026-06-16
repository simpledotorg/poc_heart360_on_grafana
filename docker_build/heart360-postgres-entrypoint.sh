#!/bin/sh
set -e

# Central vs facility node role. Set via container env (e.g. is_central_node=true in .env).
# true  = district/central node: skip matview refresh, hide manual refresh in Grafana
# false = facility node: refresh enabled (default)
: "${is_central_node:=false}"

if [ "$1" = 'postgres' ]; then
  shift
  exec /usr/local/bin/docker-entrypoint.sh postgres \
    -c "app.is_central_node=${is_central_node}" \
    "$@"
fi

exec /usr/local/bin/docker-entrypoint.sh "$@"
