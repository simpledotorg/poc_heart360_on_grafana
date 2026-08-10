# Corporate root CA certificates
#
# Drop any `*.crt` or `*.pem` root/intermediate CA files from your organisation's
# TLS-inspecting proxy into this folder.
#
# They are:
#   * copied into the system trust store when the `postgres`, `grafana` and
#     `file-processor` images are built, and
#   * bind-mounted into `/heart360-certs` at runtime so the file processor can
#     re-install them without a rebuild.
#
# This is the correct fix for:
#   SSLError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed')
#
# On Windows, export the CA from `certmgr.msc` (Base-64 encoded X.509) and save
# it here with a `.crt` extension.
#
# Nothing in this folder is committed except this README and `.gitkeep`.
# See docs/ports-and-proxy.md for the full troubleshooting guide.
