.PHONY: plugins deploy external-plugins

# Sibling plugin repos built alongside marketstore. Each must have its own
# Makefile with an `all` target that produces a .so. Build tags MUST match
# GO_BUILD_TAGS below, otherwise plugin.Open() fails with a build ID mismatch.
EXTERNAL_PLUGIN_DIRS := ../marketstore-watchlists

# Build tags applied to every `go` invocation (host binary, plugins, helpers).
#
# netgo: force the pure-Go DNS resolver. Eliminates the CGo getaddrinfo path,
# which was identified as the primary OS-thread-creation source under load
# (see plans/os-thread-accumulation.md). The host binary and all plugins MUST
# be built with the same tag set, otherwise plugin.Open() fails with a build
# ID mismatch.
GO_BUILD_TAGS ?= netgo

# -trimpath strips absolute filesystem paths (source tree + module cache) from
# compiled packages. This is REQUIRED for plugin ABI compatibility: Go hashes
# those paths into each package's build ID, so a plugin built in a different
# directory or GOPATH than the host otherwise fails plugin.Open() with
# "plugin was built with a different version of package <X>" — even when every
# dependency version is identical. It also makes release builds reproducible.
# The debug target overrides GOFLAGS to drop -trimpath so Delve can still map
# the binary back to source files.
export GOFLAGS := -tags=$(GO_BUILD_TAGS) -trimpath $(GOFLAGS)

GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
ifeq ($(GOPATH0),)
GOPATH0 := $(CURDIR)/build
endif
UTIL_PATH := github.com/alpacahq/marketstore/v4/utils

build: plugins
	GOFLAGS="$(GOFLAGS)" go build -ldflags "-s -X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" .

install:
	GOFLAGS="$(GOFLAGS)" go install -ldflags "-s -X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" .

# Debug builds keep absolute paths (no -trimpath) so Delve can resolve sources.
# This override is exported, so the contrib debug sub-makes inherit it too.
debug: export GOFLAGS := -tags=$(GO_BUILD_TAGS)
debug:
	#$(MAKE) debug -C contrib/bitmexfeeder
	#$(MAKE) debug -C contrib/gdaxfeeder
	#$(MAKE) debug -C contrib/ice
	#$(MAKE) debug -C contrib/iex
	$(MAKE) debug -C contrib/massive
	$(MAKE) debug -C contrib/ondiskagg
	$(MAKE) debug -C contrib/stream
	$(MAKE) debug -C contrib/streamreplay
	$(MAKE) debug -C contrib/watchlist
	GOFLAGS="$(GOFLAGS)" go install -gcflags="all=-N -l" -ldflags "-X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" ./...

generate:
	GOFLAGS="$(GOFLAGS)" go generate $(shell find . -path ./vendor -prune -o -name \*.go -exec grep -q go:generate {} \; -print | while read file; do echo `dirname $$file`; done | xargs)

generate-sql:
	make -C sqlparser

update:
	GOFLAGS="$(GOFLAGS)" go mod tidy

plugins:
	#$(MAKE) -C contrib/bitmexfeeder
	#$(MAKE) -C contrib/gdaxfeeder
	#${MAKE} -C contrib/ice
	#$(MAKE) -C contrib/iex
	$(MAKE) -C contrib/massive
	$(MAKE) -C contrib/ondiskagg
	$(MAKE) -C contrib/stream
	$(MAKE) -C contrib/streamreplay
	$(MAKE) -C contrib/watchlist
	$(MAKE) external-plugins

external-plugins:
	@for dir in $(EXTERNAL_PLUGIN_DIRS); do \
		if [ -d "$$dir" ]; then \
			echo "Building external plugin in $$dir"; \
			$(MAKE) -C "$$dir"; \
		else \
			echo "Skipping external plugin (not found): $$dir"; \
		fi; \
	done

fmt:
	GOFLAGS="$(GOFLAGS)" go fmt ./...

unit-test:
	# marketstore/contrib/stream/shelf/shelf_test.go fails if "-race" enabled...
	# GOFLAGS="$(GOFLAGS)" go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	GOFLAGS="$(GOFLAGS)" go test -coverprofile=coverage.txt -covermode=atomic ./...

import-csv-test:
	@tests/integ/bin/runtests.sh

integration-test-jsonrpc:
	$(MAKE) -C tests/integ test-jsonrpc

integration-test-grpc:
	$(MAKE) -C tests/integ test-grpc

integration-test-contrib:
	$(MAKE) -C tests/integ test-contrib

replication-test:
	$(MAKE) -C tests/replication test-replication

test: build
	$(MAKE) unit-test
	$(MAKE) import-csv-test
	$(MAKE) integration-test-jsonrpc
	$(MAKE) integration-test-grpc
	$(MAKE) integration-test-contrib

image:
	docker build . -t marketstore:latest -f $(DOCKER_FILE_PATH)

runimage:
	make -C tests/integ run IMAGE_NAME=alpacamarkets/marketstore.test

stopimage:
	make -C tests/integ clean IMAGE_NAME=alpacamarkets/marketstore.test

deploy:
	bash deploy.sh

push:
	docker build --build-arg tag=$(DOCKER_TAG) -t alpacamarkets/marketstore:$(DOCKER_TAG) -t alpacamarkets/marketstore:latest .
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASS)
	docker push alpacamarkets/marketstore:$(DOCKER_TAG)
	docker push alpacamarkets/marketstore:latest
