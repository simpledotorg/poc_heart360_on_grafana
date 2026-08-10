#!/bin/sh
# Resilient Python dependency installer for the HEARTS360 file processor.
#
# The original one-liner
#     pip install --break-system-packages pandas openpyxl psycopg2-binary python-calamine
# ran on every container start and hard-failed on any network hiccup. Behind a
# TLS-inspecting corporate proxy it dies with:
#     SSLError(1, '[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] ssl/tls alert handshake failure')
#
# This script:
#   1. does nothing at all if the packages are already importable (idempotent),
#   2. installs corporate root CAs from /heart360-certs,
#   3. tries an offline wheelhouse first (/heart360-wheels),
#   4. then walks an ordered list of network strategies, each addressing a
#      different documented cause of that handshake failure,
#   5. and, on total failure, prints a diagnosis instead of a traceback.
#
# Environment (all optional):
#   HEART360_PY_PACKAGES         pip package list      (default: pandas openpyxl psycopg2-binary python-calamine)
#   HEART360_PY_IMPORTS          module names to probe (default: pandas openpyxl psycopg2 python_calamine)
#   HEART360_WHEELHOUSE          offline wheel dir     (default: /heart360-wheels)
#   HEART360_APK_INSECURE_HTTP   true -> apk over http (packages remain signature-verified)
#   HEART360_TLS_INSECURE        true -> allow --trusted-host / unverified pip
#   HEART360_TLS_LEGACY          true -> relax OpenSSL to TLS1.0+ / SECLEVEL=1
#   HTTP_PROXY / HTTPS_PROXY / NO_PROXY
#   PIP_INDEX_URL / PIP_EXTRA_INDEX_URL / PIP_TRUSTED_HOST

set -u

PY_PACKAGES="${HEART360_PY_PACKAGES:-pandas openpyxl psycopg2-binary python-calamine}"
PY_IMPORTS="${HEART360_PY_IMPORTS:-pandas openpyxl psycopg2 python_calamine}"
WHEELHOUSE="${HEART360_WHEELHOUSE:-/heart360-wheels}"
TLS_INSECURE="${HEART360_TLS_INSECURE:-false}"

log() { echo "[heart360-deps] $*"; }

# The shared TLS bootstrap lives in one place. At build time it is baked into
# /usr/local/bin; at runtime it arrives via the ./scripts bind mount.
run_tls_bootstrap() {
    for candidate in /usr/local/bin/heart360-tls-bootstrap /heart360-scripts/tls_bootstrap.sh; do
        if [ -f "$candidate" ]; then
            # shellcheck disable=SC2086
            env ${1:-IGNORE_ME=1} sh "$candidate" >/dev/null 2>&1 || true
            return 0
        fi
    done
    return 0
}

deps_present() {
    command -v python3 >/dev/null 2>&1 || return 1
    for m in $PY_IMPORTS; do
        python3 -c "import ${m}" >/dev/null 2>&1 || return 1
    done
    return 0
}

pip_common_flags() {
    flags="--no-cache-dir --disable-pip-version-check --retries 3 --timeout 60"
    # PEP 668: Alpine 3.19+ marks the system interpreter as externally managed.
    if pip install --help 2>/dev/null | grep -q -- '--break-system-packages'; then
        flags="${flags} --break-system-packages"
    fi
    echo "${flags}"
}

# ---------------------------------------------------------------------------
# Step 1 - system packages (python3 + pip)
# ---------------------------------------------------------------------------
ensure_python() {
    if command -v python3 >/dev/null 2>&1 && command -v pip >/dev/null 2>&1; then
        return 0
    fi

    log "python3/pip missing, installing via apk"
    attempt=1
    while [ "${attempt}" -le 3 ]; do
        if apk add --no-cache python3 py3-pip >/dev/null 2>&1; then
            return 0
        fi
        log "apk attempt ${attempt} failed"
        if [ "${attempt}" -eq 1 ] && [ -f /etc/apk/repositories ]; then
            log "retrying apk over plain http (packages stay signature-verified)"
            sed -i 's|https://|http://|g' /etc/apk/repositories
        fi
        attempt=$((attempt + 1))
        sleep 3
    done

    command -v python3 >/dev/null 2>&1 && return 0
    return 1
}

# ---------------------------------------------------------------------------
# Step 2 - ordered install strategies
# ---------------------------------------------------------------------------
try_wheelhouse() {
    [ -d "${WHEELHOUSE}" ] || return 1
    wheel_count=0
    for w in "${WHEELHOUSE}"/*.whl; do
        [ -f "$w" ] && wheel_count=$((wheel_count + 1))
    done
    [ "${wheel_count}" -gt 0 ] || return 1

    log "strategy 1/5: offline wheelhouse ${WHEELHOUSE} (${wheel_count} wheels)"
    # shellcheck disable=SC2086
    pip install $(pip_common_flags) --no-index --find-links "${WHEELHOUSE}" ${PY_PACKAGES}
}

try_default() {
    log "strategy 2/5: standard index (honours PIP_INDEX_URL, proxy env, system CA store)"
    # shellcheck disable=SC2086
    pip install $(pip_common_flags) ${PY_PACKAGES}
}

try_explicit_proxy() {
    proxy="${HTTPS_PROXY:-${https_proxy:-${HTTP_PROXY:-${http_proxy:-}}}}"
    [ -n "${proxy}" ] || return 1
    log "strategy 3/5: explicit --proxy ${proxy}"
    # Some pip builds ignore the *_proxy environment variables. Passing --proxy
    # forces a CONNECT tunnel, which is the usual cure when a firewall silently
    # resets direct TLS and OpenSSL reports a handshake failure.
    # shellcheck disable=SC2086
    pip install $(pip_common_flags) --proxy "${proxy}" ${PY_PACKAGES}
}

try_trusted_host() {
    [ "${TLS_INSECURE}" = "true" ] || return 1
    log "strategy 4/5: --trusted-host (TLS verification off, HEART360_TLS_INSECURE=true)"
    hosts="--trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org"
    if [ -n "${PIP_TRUSTED_HOST:-}" ]; then
        hosts="${hosts} --trusted-host ${PIP_TRUSTED_HOST}"
    fi
    # shellcheck disable=SC2086
    pip install $(pip_common_flags) ${hosts} ${PY_PACKAGES}
}

try_legacy_tls() {
    log "strategy 5/5: retry with relaxed OpenSSL policy (TLS1.0+, SECLEVEL=1)"
    run_tls_bootstrap "HEART360_TLS_LEGACY=true"
    # shellcheck disable=SC2086
    pip install $(pip_common_flags) ${PY_PACKAGES}
}

diagnose() {
    log ""
    log "==================== DEPENDENCY INSTALL FAILED ===================="
    log "Could not install: ${PY_PACKAGES}"
    log ""
    log "Probing TLS reachability of pypi.org:443 ..."
    python3 - <<'PY' 2>&1 | sed 's/^/[heart360-deps]   /'
import os
import socket
import ssl

host, port = "pypi.org", 443
proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or ""
print("HTTPS_PROXY=" + (proxy or "(unset)"))
print("OpenSSL=" + ssl.OPENSSL_VERSION)
try:
    ctx = ssl.create_default_context()
    with socket.create_connection((host, port), timeout=10) as raw:
        with ctx.wrap_socket(raw, server_hostname=host) as tls:
            print("direct TLS OK, negotiated " + str(tls.version()))
except Exception as exc:
    print("direct TLS FAILED: " + type(exc).__name__ + ": " + str(exc))
PY
    log ""
    log "Most likely causes and fixes (docs/ports-and-proxy.md has the detail):"
    log "  SSLV3_ALERT_HANDSHAKE_FAILURE -> the proxy rejected the handshake."
    log "     Set HTTPS_PROXY=http://<proxy-host>:<port> in .env so pip tunnels"
    log "     through it instead of connecting directly, or set"
    log "     HEART360_TLS_LEGACY=true if the proxy only speaks older TLS."
    log "  CERTIFICATE_VERIFY_FAILED     -> TLS interception."
    log "     Export your corporate root CA and drop it into ./certs/."
    log "  No egress at all              -> use the offline wheelhouse."
    log "     pip download --only-binary=:all: --platform musllinux_1_2_x86_64 \\"
    log "       --python-version 3.12 -d ./vendor/wheels ${PY_PACKAGES}"
    log "  Internal mirror available     -> set PIP_INDEX_URL and PIP_TRUSTED_HOST."
    log "=================================================================="
}

main() {
    if deps_present; then
        log "all Python dependencies already present, nothing to do"
        return 0
    fi

    # Refresh CA trust at runtime so a new cert in ./certs works without a rebuild.
    run_tls_bootstrap

    if ! ensure_python; then
        log "FATAL: python3 could not be installed"
        return 1
    fi

    for strategy in try_wheelhouse try_default try_explicit_proxy try_trusted_host try_legacy_tls; do
        if "${strategy}"; then
            if deps_present; then
                log "dependencies installed via ${strategy}"
                return 0
            fi
            log "${strategy} reported success but imports still fail, continuing"
        fi
    done

    diagnose
    return 1
}

main "$@"
