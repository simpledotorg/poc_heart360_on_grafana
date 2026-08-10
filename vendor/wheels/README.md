# Offline Python wheelhouse
#
# When the file-processor host cannot reach PyPI at all (air-gapped site, or a
# proxy that kills the TLS handshake), pre-download the wheels on a machine that
# *does* have connectivity and copy them here:
#
#   pip download \
#     --only-binary=:all: \
#     --platform musllinux_1_2_x86_64 \
#     --python-version 3.12 \
#     --dest ./vendor/wheels \
#     pandas openpyxl psycopg2-binary python-calamine
#
# The folder is bind-mounted to /heart360-wheels; `inotify_scripts/init.sh`
# tries `pip install --no-index --find-links /heart360-wheels` first and only
# falls back to the network if that fails.
#
# Note the `--platform` value: the file-processor image is Alpine/musl based.
# Wheels built for glibc (`manylinux`) will not install there.
#
# Nothing in this folder is committed except this README and `.gitkeep`.
