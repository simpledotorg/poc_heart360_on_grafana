###
#Fixed variables
###
export DOCKER_REPO="simpledotorg"

###
#Gets BRANCH_NAME
###
export BRANCH_NAME=$(echo "${GITHUB_REF##refs/heads/}")
# Fallback to main if branch is empty (safety)
if [ -z "$BRANCH_NAME" ]; then
    export BRANCH_NAME="unknown"
fi
export BRANCH_NAME=`git rev-parse --abbrev-ref HEAD`

###
# Full commit hash
###
export COMMIT_HASH=$(git rev-parse HEAD)

###
# Get GitHub run number
###
export BUILD_NUMBER="${GITHUB_RUN_NUMBER}"
if [ -z "$BUILD_NUMBER" ]; then
    BUILD_NUMBER="0"
fi

###
# Sanitize branch name and commit hash for tag safety
###
export BRANCH_NAME=$(echo "$BRANCH_NAME" | tr '[:upper:]' '[:lower:]' | tr -cd '[:alnum:]._-' )
export COMMIT_HASH=$(echo "$COMMIT_HASH" | tr -cd '[:alnum:]')


###
# Displays variables to be able to debug manually
###
echo "using these variables for tagging"
echo "    BRANCH_NAME:  ${BRANCH_NAME}"
echo "    COMMIT_HASH:  ${COMMIT_HASH}"
echo "    BUILD_NUMBER: ${BUILD_NUMBER}"
