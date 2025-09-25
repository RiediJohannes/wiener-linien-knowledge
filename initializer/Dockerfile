FROM rust:1.87 AS builder
LABEL authors="Johannes Riedmann"
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/initializer /usr/local/bin/initializer
RUN apt-get update -y && apt install -y openssl
CMD ["initializer"]