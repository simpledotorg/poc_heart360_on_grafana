# Shared TLS / proxy bootstrap used by every image build and by the file
# processor at runtime.
#
# Usage inside a Dockerfile:
#     COPY ./scripts/tls_bootstrap.sh /usr/local/bin/heart360-tls-bootstrap
#     RUN sh /usr/local/bin/heart360-tls-bootstrap
#
# Behaviour is entirely driven by environment variables, and every step is a
# no-op when its variable is unset. A clean, unproxied build sees no change.
#
#   HEART360_CERT_DIR            where to look for corporate CAs (default /heart360-certs)
#   HEART360_APK_INSECURE_HTTP   "true" -> rewrite Alpine repos from https to http
#   HEART360_TLS_LEGACY          "true" -> relax OpenSSL to TLS1.0+ / SECLEVEL=1
#
# Why the http rewrite is acceptable: apk verifies every index and package
# against RSA keys baked into the image, so integrity does not depend on the
# transport. Only confidentiality of the download is lost. It is the standard
# Alpine workaround for middleboxes that reject the TLS handshake outright.

set -u

CERT_DIR="${HEART360_CERT_DIR:-/heart360-certs}"
APK_INSECURE_HTTP="${HEART360_APK_INSECURE_HTTP:-false}"
TLS_LEGACY="${HEART360_TLS_LEGACY:-false}"

log() { echo "[heart360-tls] $*"; }

# ---------------------------------------------------------------- CA trust ---
install_corporate_cas() {
    [ -d "$CERT_DIR" ] || return 0

    found=0
    for f in "$CERT_DIR"/*.crt "$CERT_DIR"/*.pem "$CERT_DIR"/*.cer; do
        [ -f "$f" ] && found=$((found + 1))
    done
    [ "$found" -gt 0 ] || return 0

    log "installing ${found} corporate CA file(s) from ${CERT_DIR}"

    if [ -d /etc/pki/ca-trust/source/anchors ]; then
        target=/etc/pki/ca-trust/source/anchors
    else
        target=/usr/local/share/ca-certificates
        mkdir -p "$target"
    fi

    for f in "$CERT_DIR"/*.crt "$CERT_DIR"/*.pem "$CERT_DIR"/*.cer; do
        [ -f "$f" ] || continue
        base=$(basename "$f")
        # update-ca-certificates only picks up files ending in .crt
        cp "$f" "${target}/heart360-${base%.*}.crt" 2>/dev/null || true
    done

    if command -v update-ca-certificates >/dev/null 2>&1; then
        update-ca-certificates >/dev/null 2>&1 || log "warning: update-ca-certificates failed"
    elif command -v update-ca-trust >/dev/null 2>&1; then
        update-ca-trust extract >/dev/null 2>&1 || log "warning: update-ca-trust failed"
    else
        log "warning: no CA update tool found; certs copied to ${target} only"
    fi
}

# ------------------------------------------------------------ apk over http ---
relax_apk_transport() {
    [ "$APK_INSECURE_HTTP" = "true" ] || return 0
    [ -f /etc/apk/repositories ] || return 0
    log "rewriting /etc/apk/repositories to http (packages stay signature-verified)"
    sed -i 's|https://|http://|g' /etc/apk/repositories
}

# --------------------------------------------------------- legacy TLS knobs ---
# Targets SSLV3_ALERT_HANDSHAKE_FAILURE caused by middleboxes that cannot
# negotiate the cipher suites and security level OpenSSL 3 defaults to.
relax_openssl() {
    [ "$TLS_LEGACY" = "true" ] || return 0

    conf=""
    for candidate in /etc/ssl/openssl.cnf /usr/lib/ssl/openssl.cnf /etc/pki/tls/openssl.cnf; do
        if [ -f "$candidate" ]; then conf="$candidate"; break; fi
    done
    if [ -z "$conf" ]; then
        log "warning: openssl.cnf not found, skipping legacy TLS tuning"
        return 0
    fi

    if grep -q 'heart360_legacy_tls' "$conf" 2>/dev/null; then
        return 0
    fi

    log "relaxing OpenSSL policy in ${conf} (MinProtocol=TLSv1.0, SECLEVEL=1)"
    cat >>"$conf" <<'EOF'

# --- heart360_legacy_tls ---
# Added by scripts/tls_bootstrap.sh when HEART360_TLS_LEGACY=true.
# Allows handshakes with older or non-conformant TLS-inspecting proxies.
[openssl_init]
ssl_conf = heart360_ssl_conf

[heart360_ssl_conf]
system_default = heart360_ssl_default

[heart360_ssl_default]
MinProtocol = TLSv1.0
CipherString = DEFAULT@SECLEVEL=1
Options = UnsafeLegacyRenegotiation
EOF
}

install_corporate_cas
relax_apk_transport
relax_openssl

exit 0
