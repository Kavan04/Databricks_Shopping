# Shopping Sessions: Rust pipeline. Multi-stage: build with cargo, run on slim Debian.
FROM rust:1-bookworm AS build
ARG TARGETARCH
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src src
RUN if [ "$TARGETARCH" = "amd64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --release && cp target/release/shopping /shopping

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopping /app/shopping
WORKDIR /app
# TEMPORARY diagnostic build: prints scorer facts and timings, then fails the
# dev check on purpose so the logs are shown. Revert after one submission.
ENV FM_DIAG=1
CMD ["/app/shopping"]
