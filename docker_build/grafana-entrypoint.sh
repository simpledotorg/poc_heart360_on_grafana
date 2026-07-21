#!/bin/sh
set -e

SRC="/etc/grafana/provisioning"
DEST="/tmp/grafana-provisioning-runtime"

rm -rf "$DEST"
cp -r "$SRC" "$DEST"

if [ "$IS_CENTRAL_NODE" = "true" ]; then
  rm -f "$DEST/dashboards/dashboard_provider_admin.yml"
fi

export GF_PATHS_PROVISIONING="$DEST"
exec /run.sh "$@"
