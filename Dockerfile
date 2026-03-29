# Stage 1: build 
# use official Go image to compile our code
FROM golang:1.25.4-alpine AS builder

# set working directory inside container
WORKDIR /app

# copy go.mod and go.sum first
# Docker caches this layer  only re-downloads deps if these change
COPY go.mod go.sum ./
RUN go mod download

# copy all source code
COPY . .

# build server binary
RUN go build -o bin/server ./cmd/server

# build client binary
RUN go build -o bin/client ./cmd/client

# ─Stage 2: final image 
# use tiny alpine image — no Go compiler needed at runtime
# this makes final image much smaller
FROM alpine:latest

WORKDIR /app

# copy compiled binaries from builder stage
COPY --from=builder /app/bin/server ./bin/server
COPY --from=builder /app/bin/client ./bin/client

# create data directories
RUN mkdir -p /data/input /data/output

# default command — overridden by docker-compose
CMD ["./bin/server"]
