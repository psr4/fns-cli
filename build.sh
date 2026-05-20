#!/bin/bash

set -e

docker run --rm -it -v ./:/app rust:1.86 bash -c "cd /app && cargo build --release"