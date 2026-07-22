#!/bin/sh
set -e

SRC="/etc/grafana/provisioning"
DEST="/tmp/grafana-provisioning-runtime"

DASHBOARDS_DIR="$DEST/dashboards/HEARTS360 Dashboards"

rm -rf "$DEST"
cp -r "$SRC" "$DEST"

if [ "$IS_CENTRAL_NODE" = "true" ]; then
  rm -f "$DEST/dashboards/dashboard_provider_admin.yml"
  rm -f "$DASHBOARDS_DIR/heart360.overdue.patients.json"
fi

IS_CN_VALUE="${IS_CENTRAL_NODE:-false}"
for f in \
  "$DASHBOARDS_DIR/heart360.hypertension.program.json" \
  "$DASHBOARDS_DIR/heart360.diabetes.program.json" \
  "$DASHBOARDS_DIR/heart360.overdue.patients.json"; do
  if [ -f "$f" ]; then
    sed -i "s/__IS_CENTRAL_NODE_PLACEHOLDER__/${IS_CN_VALUE}/g" "$f"
  fi
done

export GF_PATHS_PROVISIONING="$DEST"
exec /run.sh "$@"
