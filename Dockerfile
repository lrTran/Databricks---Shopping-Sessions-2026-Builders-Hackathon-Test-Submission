# Shopping Sessions: Rust pipeline.
# The scorers build this natively on linux/amd64 (Intel Xeon, Cascade Lake or newer).
# Build time is not scored; the build has network for up to 10 minutes.

FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
# Build dependencies first, in their own layer, so code edits rebuild fast.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
 && cargo build --release --locked \
 && rm -rf src target/release/deps/sessions-* target/release/.fingerprint/sessions-* target/release/sessions*
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
# The scorer runs as uid 1000 with a read-only root; only /output, /scratch, /tmp are writable.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
USER 1000:1000
# No arguments: reads $DATA_DIR, writes $OUTPUT_DIR, uses $NCPU threads.
CMD ["/app/sessions"]
