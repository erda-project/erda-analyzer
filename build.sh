#!/bin/bash

set -o errexit -o nounset -o pipefail

output="$(pwd)/$1-image"
echo "${output}"

cd repo

v="$(head -n 1 VERSION)"

v="${v}-$(date '+%Y%m%d')-$(git rev-parse --short HEAD)"

nexusUrl="$(echo $BP_NEXUS_URL)"
echo $nexusUrl
nexusUsername="$(echo $BP_NEXUS_USERNAME)"
echo $nexusUsername
nexusPassword="$(echo $BP_NEXUS_PASSWORD)"
echo $nexusPassword

sed -i 's^{{BP_NEXUS_URL}}^'"${nexusUrl}"'^g' /root/.m2/settings.xml
sed -i 's^{{BP_NEXUS_USERNAME}}^'"${nexusUsername}"'^g' /root/.m2/settings.xml
sed -i 's^{{BP_NEXUS_PASSWORD}}^'"${nexusPassword}"'^g' /root/.m2/settings.xml

image_vpc="registry-vpc.cn-hangzhou.aliyuncs.com/terminus/dice-$1:${v}"
image="registry.cn-hangzhou.aliyuncs.com/terminus/dice-$1:${v}"

mvn clean package -pl $1 -am -B -DskipTests

docker login -u "${TERMINUS_DOCKERHUB_ALIYUN_USERNAME}" -p "${TERMINUS_DOCKERHUB_ALIYUN_PASSWORD}" registry-vpc.cn-hangzhou.aliyuncs.com

docker build -t "${image_vpc}" \
    --label "branch=$(git rev-parse --abbrev-ref HEAD)" \
    --label "commit=$(git rev-parse HEAD)" \
    --label "build-time=$(date '+%Y-%m-%d %T%z')" \
    --build-arg APP=$1 \
    -f Dockerfile .

docker push "${image_vpc}"

cat > "${METAFILE}" <<EOF
image=${image}
EOF