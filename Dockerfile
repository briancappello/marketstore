#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.26.5-bookworm AS builder
ARG tag=latest
ARG INCLUDE_PLUGINS=true
ENV DOCKER_TAG=$tag
ENV GOPATH=/go

WORKDIR /go/src/github.com/alpacahq/marketstore/
ADD ./ ./
RUN if [ "$INCLUDE_PLUGINS" = "true" ] ; then make build plugins ; else make build ; fi

#
# STAGE 2
#
# Create final image
#
FROM debian:12-slim
WORKDIR /

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/src/github.com/alpacahq/marketstore/marketstore /bin/
COPY --from=builder /go/bin /bin/
COPY --from=builder /go/src/github.com/alpacahq/marketstore/contrib/ice/ca-sync-*.sh /bin/

ENV GOPATH=/

RUN ["marketstore", "init"]
RUN mv mkts.yml /etc/
# Pre-create the workdir used by the test harness (docker/podman create
# --workdir /project). docker cp auto-creates it, podman cp does not, so
# making it part of the image keeps `create -> cp -> start` portable.
RUN mkdir -p /project
VOLUME /data
EXPOSE 5993

ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "/etc/mkts.yml"]
