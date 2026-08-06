#!/bin/bash

set -e

docker volume create fns-cli-cargo-registry >/dev/null
docker volume create fns-cli-cargo-git >/dev/null

docker run --rm \
  -v "$(pwd):/app" \
  -v fns-cli-cargo-registry:/usr/local/cargo/registry \
  -v fns-cli-cargo-git:/usr/local/cargo/git \
  -w /app \
  rust:1.86 \
  cargo build --release

docker build . -t fns-cli

docker run -d -v /vol1/1000/obsidian:/vol1/1000/obsidian -v ./:/service --name fns-cli --network=hermes_default --restart=always fns-cli
