# Sequoia's nmsr-aas image. Build it on the serving host from a directory holding this file and
# the production config.toml; the application source is fetched at a pinned commit.
ARG BASE=rust:slim-bookworm@sha256:86234e857a413004348b471d11a4ebe1aa92d2eae2b6b143cac68fd1ac3c48be

FROM ${BASE} AS builder

ARG NMSR_REPOSITORY=https://github.com/OneNoted/nmsr-rs.git
ARG NMSR_COMMIT=0a4c242b8ff41da42d9ec00cf2ab9a3aba0a264a
ARG RUST_TOOLCHAIN=nightly-2026-07-08
ARG TARGET_CPU=native

# Upgrade before any download so git, cargo and rustup use the current Debian trust store.
RUN apt-get update -y && apt-get upgrade -y \
    && apt-get --no-install-recommends install -y git libssl-dev pkg-config ca-certificates \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge 20250419~deb12u2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/nmsr-rs

RUN git init -q . && git fetch -q --depth 1 "$NMSR_REPOSITORY" "$NMSR_COMMIT" \
    && git checkout -q FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "$NMSR_COMMIT"

RUN rustup toolchain install "$RUST_TOOLCHAIN" --profile minimal --no-self-update \
    && RUSTUP_TOOLCHAIN="$RUST_TOOLCHAIN" RUSTFLAGS="-Ctarget-cpu=$TARGET_CPU" \
       cargo build --locked --release --bin nmsr-aas --features ears --package nmsr-aas

FROM ${BASE}

RUN apt-get update -y && apt-get upgrade -y \
    && apt-get --no-install-recommends install -y mesa-vulkan-drivers libssl3 ca-certificates curl \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge 20250419~deb12u2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /nmsr

COPY --from=builder /tmp/nmsr-rs/target/release/nmsr-aas /nmsr/nmsr-aas
COPY ./config.toml /nmsr/config.toml

ENV NMSR_USE_SMAA=1
ENV NMSR_SAMPLE_COUNT=1
ENV WGPU_BACKEND=vulkan
ENV RUST_BACKTRACE=1
ENV RUST_LOG=info

RUN chmod +x /nmsr/nmsr-aas

EXPOSE 8080

CMD ["/nmsr/nmsr-aas", "-c", "config.toml"]
