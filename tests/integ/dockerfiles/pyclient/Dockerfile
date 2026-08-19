# Pinned to a Python 3.11 miniconda: the pinned pytest 3.6.1 imports the `imp`
# module, which was removed in Python 3.12. `:latest` now ships Python 3.14 and
# breaks test collection.
FROM continuumio/miniconda3:23.10.0-1

# docker CLI (client only, no daemon) so tests can control sibling containers
# through a bind-mounted engine socket. This works against both a real docker
# socket and podman's docker-compatible API socket.
ARG DOCKER_CLI_VERSION=27.3.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
       | tar -xz -C /usr/local/bin --strip-components=1 docker/docker \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /
RUN pip install -r /requirements.txt

WORKDIR /project
