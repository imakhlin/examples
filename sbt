#!/bin/bash
export SBT_CREDENTIALS=`dirname $0`/project/credentials

ART_URL="http://iguazio_ro:AP6zvYnUy6UqWtgfE7uidNWhFF5@artifactory.iguazeng.com:8081/artifactory"
SEARCH_VERSION="2.8_stable"

if [ "$VERSION" = "" ]; then
  VERSION=$(curl --silent ${ART_URL}/iguazio_tags/${SEARCH_VERSION}/version.txt)
fi

if [ "$VERSION" = "" ]; then
  VERSION=$SEARCH_VERSION
fi

JAVA_TO_USE=''
if [ -n "$JAVA_HOME" ]; then
    JAVA_TO_USE=${JAVA_HOME}/bin/java
else
    JAVA_TO_USE=java
fi

echo "using java="$JAVA_TO_USE
echo "attempt to build with version "$VERSION

${JAVA_TO_USE} $JAVA_OPTS -Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled \
  -Ddefault.version=${VERSION} \
  -Dsbt.override.build.repos=true -Dsbt.repository.config=`dirname $0`/project/repositories \
  -jar `dirname $0`/sbt-launch.jar "$@"
