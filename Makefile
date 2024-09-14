# Copyright (c) 2021 Terminus, Inc.
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

DOCKER_REGISTRY ?= registry.erda.cloud/erda
PLATFORM ?= linux/amd64,linux/arm64
VERSION = $(shell ./make-version.sh tag)
BUILD_TIME := $(shell date '+%Y-%m-%d %T%z')
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
COMMIT_ID := $(shell git rev-parse HEAD 2>/dev/null)
IMAGE_TAG ?= $(VERSION)
DOCKER_IMAGE ?= $(DOCKER_REGISTRY)/erda-$(APP):$(IMAGE_TAG)
APP ?=

ifndef APP
  $(error APP is required. Usage: make build-image APP=<NAME>)
endif

build:
	@mvn clean package -pl ${APP} -am -B -DskipTests

image:
	@docker buildx build --pull \
		--platform ${PLATFORM} \
		--label "branch=${BRANCH}" \
		--label "commit=${COMMIT_ID}" \
		--label "build-time=$(BUILD_TIME)" \
		--build-arg "APP=$(APP)" \
		-t "$(DOCKER_IMAGE)" \
		-f Dockerfile . $(EXTRA_ARGS)

build-image:
	@$(MAKE) image EXTRA_ARGS="--load"

build-push-image:
	@$(MAKE) image EXTRA_ARGS="--push"
