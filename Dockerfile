FROM rust:1-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd -r -u 10001 relay && mkdir -p /data && chown relay /data
COPY --from=build /app/target/release/dbcloudrelay /usr/local/bin/dbcloudrelay
USER relay
ENV RELAY_DATA_DIR=/data PORT=8080
EXPOSE 8080
VOLUME ["/data"]
CMD ["dbcloudrelay"]
