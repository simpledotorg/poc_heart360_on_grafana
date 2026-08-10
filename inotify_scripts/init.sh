#!/bin/sh
# HEARTS360 file-processor entrypoint.
#
# Previously this unconditionally ran `apk update && apk upgrade && apk add ...`
# followed by a `pip install`, on every single container start. That made the
# service require working internet egress at boot, and it died outright behind
# a TLS-inspecting proxy with:
#     SSLError(1, '[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] ssl/tls alert handshake failure')
#
# Bootstrapping is now idempotent, offline-capable and non-fatal by default:
# the watcher still starts so an already-provisioned image keeps working, and
# the failure is reported with an actionable diagnosis instead of a traceback.
#
#   HEART360_SKIP_BOOTSTRAP=true   skip dependency bootstrap entirely
#                                  (correct for pre-baked images)
#   HEART360_REQUIRE_DEPS=true     make a failed bootstrap fatal instead of
#                                  starting a watcher that cannot ingest
set -u

SCRIPT_DIR=$(dirname "$0")

echo "[heart360-init] Doing the init ..."

if [ "${HEART360_SKIP_BOOTSTRAP:-false}" = "true" ]; then
    echo "[heart360-init] HEART360_SKIP_BOOTSTRAP=true - skipping dependency bootstrap"
elif sh "${SCRIPT_DIR}/install_python_deps.sh"; then
    echo "[heart360-init] dependencies ready"
else
    echo "[heart360-init] WARNING: dependency bootstrap failed."
    echo "[heart360-init] Uploaded files will NOT be ingested until this is fixed."
    echo "[heart360-init] See docs/ports-and-proxy.md"
    if [ "${HEART360_REQUIRE_DEPS:-false}" = "true" ]; then
        echo "[heart360-init] HEART360_REQUIRE_DEPS=true - exiting."
        exit 1
    fi
fi

echo "[heart360-init] starting inotify watcher on ${INOTIFY_TARGET:-/data}"
exec bash /docker-entrypoint.sh inotify-script
