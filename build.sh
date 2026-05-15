#!/bin/bash

set -e

docker run --rm -it -v ./:/app rust:1.86 bash -c "cd /app && cargo build --release"

docker build . -t fns-cli

docker run -d -v /vol1/1000/obsidian:/vol1/1000/obsidian -v ./:/service --name fns-cli --network=hermes_default --restart=always fns-cli