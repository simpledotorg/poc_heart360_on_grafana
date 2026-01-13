. getenv.sh


###
# Tags versions
###
docker tag heart360tk-grafana       ${DOCKER_REPO}/heart360tk-grafana:latest
docker tag heart360tk-postgresql    ${DOCKER_REPO}/heart360tk-postgresql:latest
docker tag heart360tk-fileprocessor ${DOCKER_REPO}/heart360tk-fileprocessor:latest


docker tag heart360tk-grafana       ${DOCKER_REPO}/heart360tk-grafana:${BRANCH_NAME}
docker tag heart360tk-postgresql    ${DOCKER_REPO}/heart360tk-postgresql:${BRANCH_NAME}
docker tag heart360tk-fileprocessor ${DOCKER_REPO}/heart360tk-fileprocessor:${BRANCH_NAME}

docker tag heart360tk-grafana       ${DOCKER_REPO}/heart360tk-grafana:${BRANCH_NAME}.${BUILD_NUMBER}
docker tag heart360tk-postgresql    ${DOCKER_REPO}/heart360tk-postgresql:${BRANCH_NAME}.${BUILD_NUMBER}
docker tag heart360tk-fileprocessor ${DOCKER_REPO}/heart360tk-fileprocessor:${BRANCH_NAME}.${BUILD_NUMBER}


docker tag heart360tk-grafana       ${DOCKER_REPO}/heart360tk-grafana:${BRANCH_NAME}.${COMMIT_HASH}
docker tag heart360tk-postgresql    ${DOCKER_REPO}/heart360tk-postgresql:${BRANCH_NAME}.${COMMIT_HASH}
docker tag heart360tk-fileprocessor ${DOCKER_REPO}/heart360tk-fileprocessor:${BRANCH_NAME}.${COMMIT_HASH}

docker tag heart360tk-grafana       ${DOCKER_REPO}/heart360tk-grafana:${COMMIT_HASH}
docker tag heart360tk-postgresql    ${DOCKER_REPO}/heart360tk-postgresql:${COMMIT_HASH}
docker tag heart360tk-fileprocessor ${DOCKER_REPO}/heart360tk-fileprocessor:${COMMIT_HASH}
