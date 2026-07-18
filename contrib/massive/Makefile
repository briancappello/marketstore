GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
ifeq ($(GOPATH0),)
GOPATH0 := $(CURDIR)/../../build
endif

all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/massive.so -buildmode=plugin .
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/massive_backfiller ./backfill/rest/cmd/
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/massive_flatfile_backfiller ./backfill/flatfiles/cmd/

debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/massive.so -buildmode=plugin .

# race-test runs the race detector on the dynamic-subscription packages only.
# Scoped to ws/ and subscription/ so it never pulls in the -race-incompatible
# contrib/stream/shelf code. Run this for any change touching ws/ or subscription/.
race-test:
	go test -race ./ws/... ./subscription/...
