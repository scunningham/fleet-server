# syntax=docker/dockerfile:experimental

############################
# STEP 0 build environment
############################
# golang:1.16.4-alpine3.13
FROM golang@sha256:0dc62c5cc2d97657c17ff3bc0224214e10226e245c94317e352ee8a2c54368b4 as buildenv

# Install git + SSL ca certificates.
# Git is required for fetching the dependencies.
# Ca-certificates is required to call HTTPS endpoints.
RUN apk update && apk add --no-cache git openssh-client ca-certificates tzdata && update-ca-certificates

# download public key for github.com
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Create appuser
RUN adduser -D -g '' appuser

# Setup workdir in the build image
WORKDIR $GOPATH/src/mypackage/myapp/

# Allow our private modules
ENV GOPRIVATE=github.com/elastic/*
RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

COPY go.mod .
COPY go.sum .

# Download and validate packages, these will get cached as a layer so should only happen once.
RUN --mount=type=ssh go mod download -x
RUN --mount=type=ssh go mod verify

# Dupe the source code in the target
COPY . .

# Setup env
ENV CGO_ENABLED=0
ENV GOOS=linux

############################
# STEP 1 build executable binary
############################
from buildenv as builder

# Build the binary, use ldflags to minimize executable size
ARG LDFLAGS
RUN --mount=type=cache,target=/root/.cache/go-build go build -trimpath -ldflags="$LDFLAGS" -o /go/bin/fleet ./cmd/fleet/


