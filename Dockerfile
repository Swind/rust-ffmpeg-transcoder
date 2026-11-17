FROM rust:1.93-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg ca-certificates curl vainfo mesa-va-drivers libva-drm2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 transcoder \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent transcoder \
    && mkdir -p /etc/transcoder
COPY --from=builder /build/target/release/transcoder-server /usr/local/bin/transcoder-server
COPY --from=builder /build/target/release/transcoder-client /usr/local/bin/transcoder-client
COPY config.example.toml /etc/transcoder/config.toml
COPY profiles /etc/transcoder/profiles
USER 10001:10001
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=3s --start-period=45s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/transcoder-server"]
CMD ["--config", "/etc/transcoder/config.toml"]
