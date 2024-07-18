
ARG RUST_VERSION=1.76

FROM --platform=$BUILDPLATFORM docker.io/library/rust:${RUST_VERSION} AS builder

COPY ./container/cargo/config.toml ./.cargo/config.toml

RUN apt update && apt upgrade -y && apt install -y \
    cmake \
    libssl-dev \
    pkg-config \
    protobuf-compiler

WORKDIR /up

COPY ./ .

RUN rustup target add "x86_64-unknown-linux-gnu"; \
    cargo build --release --target="x86_64-unknown-linux-gnu" --example client_example; \
    cp /up/target/x86_64-unknown-linux-gnu/release/examples/client_example /up/service

################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
#
# The example below uses the debian bullseye image as the foundation for running the app.
# By specifying the "bullseye-slim" tag, it will also use whatever happens to be the
# most recent version of that tag when you build your Dockerfile. If
# reproducability is important, consider using a digest
# (e.g., debian@sha256:ac707220fbd7b67fc19b112cee8170b41a9e97f703f588b2cdbbcdcecdd8af57).
FROM --platform=$TARGETPLATFORM docker.io/library/debian:bullseye-slim AS final

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#user
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "10001" \
    appuser

WORKDIR /up

COPY --from=builder /up/service /up
COPY --from=builder /up/examples/config /up/config

# Start the up client example
CMD ["/up/service", "/up/config/client_config.json"]
