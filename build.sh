#!/bin/bash

set -o errexit -o nounset -o pipefail

v="$(./make-version.sh tag)"

sed -i 's^{{BP_NEXUS_URL}}^'"${BP_NEXUS_URL}"'^g' /root/.m2/settings.xml
sed -i 's^{{BP_NEXUS_USERNAME}}^'"${BP_NEXUS_USERNAME}"'^g' /root/.m2/settings.xml
sed -i 's^{{BP_NEXUS_PASSWORD}}^'"${BP_NEXUS_PASSWORD}"'^g' /root/.m2/settings.xml

image="${DOCKER_REGISTRY}/erda-$1:${v}"

mvn clean package -pl $1 -am -B -DskipTests

echo "${image}"

docker login -u "${DOCKER_REGISTRY_USERNAME}" -p "${DOCKER_REGISTRY_PASSWORD}" ${DOCKER_REGISTRY}

docker build -t "${image}" \
    --label "branch=$(git rev-parse --abbrev-ref HEAD)" \
    --label "commit=$(git rev-parse HEAD)" \
    --label "build-time=$(date '+%Y-%m-%d %T%z')" \
    --build-arg APP=$1 \
    -f Dockerfile .

docker push "${image}"

cat > "${METAFILE}" <<EOF
image=${image}
EOF
