GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
ifeq ($(GOPATH0),)
GOPATH0 := $(CURDIR)/../../build
endif
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/ice ice.go
