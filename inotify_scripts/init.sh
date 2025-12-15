echo "Doing the init ..."

apk update
apk upgrade
apk add python3

bash /docker-entrypoint.sh inotify-script
