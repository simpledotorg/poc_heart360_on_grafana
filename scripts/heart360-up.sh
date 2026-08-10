#!/usr/bin/env bash
# HEARTS360 Toolkit - port-aware launcher (Linux / macOS).
#
# Picks a free host port for every published service, records the choice in
# .env, then starts the stack. Solves the most common first-run failure:
#
#   Error response from daemon: driver failed programming external
#   connectivity on endpoint grafana: Bind for 0.0.0.0:3000 failed:
#   port is already allocated
#
# Usage:
#   ./scripts/heart360-up.sh                 # pick free ports and start
#   ./scripts/heart360-up.sh --check         # report only, change nothing
#   ./scripts/heart360-up.sh --no-start      # write .env, do not start
#   ./scripts/heart360-up.sh --build         # extra args go to docker compose up
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

DO_START=1
CHECK_ONLY=0
COMPOSE_ARGS=()

for arg in "$@"; do
    case "$arg" in
        --check)    CHECK_ONLY=1; DO_START=0 ;;
        --no-start) DO_START=0 ;;
        -h|--help)  sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^#\{1,\} \{0,1\}//'; exit 0 ;;
        *)          COMPOSE_ARGS+=("$arg") ;;
    esac
done

# key|env var|default port|description
SERVICES="grafana|HEART360_GRAFANA_PORT|3000|Grafana dashboards
postgres|HEART360_POSTGRES_PORT|5432|PostgreSQL
filebrowser|HEART360_FILEBROWSER_PORT|8080|FileBrowser upload UI
pgadmin|HEART360_PGADMIN_PORT|5050|pgAdmin"

CONTAINER_VARS="HEART360_GRAFANA_CONTAINER HEART360_POSTGRES_CONTAINER HEART360_FILEBROWSER_CONTAINER HEART360_PGADMIN_CONTAINER HEART360_FILEPROC_CONTAINER"
DEFAULT_CONTAINERS="grafana postgres filebrowser-quantum pgadmin file-upload-trigger"

info() { printf '  %s\n' "$*"; }
ok()   { printf '  [ ok ] %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }

# --------------------------------------------------------------- .env access -
env_get() {
    local key="$1" line
    [ -f "$ENV_FILE" ] || return 1
    line="$(grep -E "^[[:space:]]*${key}=" "$ENV_FILE" | tail -n 1 || true)"
    [ -n "$line" ] || return 1
    printf '%s' "${line#*=}"
}

env_set() {
    local key="$1" value="$2" tmp
    touch "$ENV_FILE"
    if grep -qE "^[[:space:]]*${key}=" "$ENV_FILE"; then
        # Portable in-place edit; BSD sed on macOS handles -i differently.
        tmp="$(mktemp)"
        sed -E "s|^[[:space:]]*${key}=.*|${key}=${value}|" "$ENV_FILE" >"$tmp"
        mv "$tmp" "$ENV_FILE"
    else
        printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
    fi
}

# ------------------------------------------------------------- port probing --
# Ports already published by this stack's own containers must not count as
# collisions, otherwise every restart would shuffle the URLs.
own_ports() {
    command -v docker >/dev/null 2>&1 || return 0
    local names="$DEFAULT_CONTAINERS" var val n
    local filters=()
    for var in $CONTAINER_VARS; do
        if val="$(env_get "$var")"; then names="$names $val"; fi
    done
    for n in $names; do filters+=(--filter "name=^/${n}$"); done
    docker ps "${filters[@]}" --format '{{.Ports}}' 2>/dev/null \
        | tr ',' '\n' \
        | sed -nE 's/.*:([0-9]+)->.*/\1/p' \
        | sort -u
}

port_in_use() {
    local port="$1"
    # ss and lsof report every address family, which matters because Docker
    # commonly publishes on the IPv6 wildcard only.
    if command -v ss >/dev/null 2>&1; then
        ss -ltnH "sport = :${port}" 2>/dev/null | grep -q .
        return $?
    fi
    if command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
        return $?
    fi
    if command -v netstat >/dev/null 2>&1; then
        netstat -an 2>/dev/null | grep -qE "[.:]${port}[[:space:]]+.*LISTEN"
        return $?
    fi
    if command -v python3 >/dev/null 2>&1; then
        if python3 -c "
import socket, sys
s = socket.socket()
try:
    s.bind(('', ${port}))
except OSError:
    sys.exit(1)
finally:
    s.close()
" >/dev/null 2>&1; then
            return 1
        fi
        return 0
    fi
    warn "no port-probing tool found (ss/lsof/netstat/python3); assuming ${port} is free"
    return 1
}

next_free_port() {
    local start="$1" reserved="$2" candidate="$start"
    while [ "$candidate" -le 65535 ]; do
        if ! port_in_use "$candidate" && ! printf '%s\n' "$reserved" | grep -qx "$candidate"; then
            printf '%s' "$candidate"
            return 0
        fi
        candidate=$((candidate + 1))
    done
    return 1
}

# --------------------------------------------------------------------- main --
printf '\nHEARTS360 Toolkit - port preflight\n\n'

if [ ! -f "$ENV_FILE" ] && [ "$CHECK_ONLY" -eq 0 ] && [ -f "${REPO_ROOT}/.env.example" ]; then
    cp "${REPO_ROOT}/.env.example" "$ENV_FILE"
    ok "created .env from .env.example"
fi

OWN="$(own_ports || true)"
if [ -n "$OWN" ]; then
    info "ports already held by this stack: $(echo "$OWN" | tr '\n' ' ')"
fi

RESERVED=""
CHANGED=0
declare -A RESOLVED=()

while IFS='|' read -r key var default desc; do
    [ -n "${key:-}" ] || continue

    wanted="$(env_get "$var" || printf '%s' "$default")"
    case "$wanted" in ''|*[!0-9]*) wanted="$default" ;; esac

    label="$(printf '%-24s' "$desc")"

    if printf '%s\n' "$OWN" | grep -qx "$wanted"; then
        ok "${label} ${wanted} (already served by this stack)"
        RESERVED="${RESERVED}${wanted}"$'\n'
        RESOLVED["$key"]="$wanted"
        continue
    fi

    if port_in_use "$wanted"; then
        if ! chosen="$(next_free_port "$((wanted + 1))" "$RESERVED")"; then
            warn "${desc}: no free port found at or above ${wanted}"
            exit 1
        fi
        warn "${label} ${wanted} is busy -> using ${chosen}"
        CHANGED=1
    else
        chosen="$wanted"
        ok "${label} ${chosen}"
    fi

    [ "$CHECK_ONLY" -eq 0 ] && env_set "$var" "$chosen"
    RESERVED="${RESERVED}${chosen}"$'\n'
    RESOLVED["$key"]="$chosen"
done <<< "$SERVICES"

if [ "$CHECK_ONLY" -eq 0 ] && [ "$CHANGED" -eq 1 ]; then
    # Keep Grafana's generated links pointing at the port people actually use.
    env_set HEART360_GRAFANA_ROOT_URL "http://localhost:${RESOLVED[grafana]}/"
fi

if [ "$CHECK_ONLY" -eq 1 ]; then
    printf '\n  (--check: .env was not modified)\n\n'
    exit 0
fi

if [ "$DO_START" -eq 1 ]; then
    printf '\n  Starting stack...\n\n'
    ( cd "$REPO_ROOT" && docker compose up -d ${COMPOSE_ARGS[@]+"${COMPOSE_ARGS[@]}"} )
fi

cat <<EOF

  HEARTS360 Toolkit is configured on:

    Dashboards   http://localhost:${RESOLVED[grafana]}
    Upload files http://localhost:${RESOLVED[filebrowser]}
    pgAdmin      http://localhost:${RESOLVED[pgadmin]}
    PostgreSQL   localhost:${RESOLVED[postgres]}

  Ports are recorded in .env and will be reused on the next start.
  Trouble connecting or building? See docs/ports-and-proxy.md

EOF
