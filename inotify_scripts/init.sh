echo "Doing the init ..."

apk update
apk upgrade
apk add python3 py3-pip
pip install --break-system-packages pandas openpyxl psycopg2-binary python-calamine

bash /docker-entrypoint.sh inotify-script



