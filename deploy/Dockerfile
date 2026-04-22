FROM rust:1.95-trixie AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY sql ./sql
RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/mercek /usr/local/bin/mercek
COPY sql /app/sql
EXPOSE 5514/udp 5514 6514 9000
CMD ["mercek"]
