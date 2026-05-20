# Build stage
FROM rust:1.86 AS builder

ENV TZ=Asia/Shanghai

COPY ./target/release/fns-cli /usr/local/bin/

WORKDIR /app

ENTRYPOINT ["fns-cli"]
CMD ["-c", "/service/config.yaml", "run"]
