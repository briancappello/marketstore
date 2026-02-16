GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
ifeq ($(GOPATH0),)
GOPATH0 := $(CURDIR)/../../build
endif
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/polygon.so -buildmode=plugin .
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/polygon_backfiller backfill/backfiller/backfiller.go

debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/polygon.so -buildmode=plugin .
