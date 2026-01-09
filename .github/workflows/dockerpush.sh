. getenv.sh


###
# Pushes Versions
###
docker push ${DOCKER_REPO}/heart360tk-grafana:latest
docker push ${DOCKER_REPO}/heart360tk-postgresql:latest

docker push ${DOCKER_REPO}/heart360tk-grafana:${BRANCH_NAME}
docker push ${DOCKER_REPO}/heart360tk-postgresql:${BRANCH_NAME}

docker push ${DOCKER_REPO}/heart360tk-grafana:${BRANCH_NAME}.${BUILD_NUMBER}
docker push ${DOCKER_REPO}/heart360tk-postgresql:${BRANCH_NAME}.${BUILD_NUMBER}


docker push ${DOCKER_REPO}/heart360tk-grafana:${BRANCH_NAME}.${COMMIT_HASH}
docker push ${DOCKER_REPO}/heart360tk-postgresql:${BRANCH_NAME}.${COMMIT_HASH}

docker push ${DOCKER_REPO}/heart360tk-grafana:${COMMIT_HASH}
docker push ${DOCKER_REPO}/heart360tk-postgresql:${COMMIT_HASH}

