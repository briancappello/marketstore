GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
ifeq ($(GOPATH0),)
GOPATH0 := $(CURDIR)/../../build
endif
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/alpaca.so -buildmode=plugin .

debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/alpaca.so -buildmode=plugin .
