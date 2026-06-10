#===================================================
# Stage 1: Builder
#===================================================
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcompat gcc

WORKDIR /usr/src/ergosphere
COPY . .

RUN cargo build --release

#===================================================
# Stage 2: Runner
#===================================================
FROM  alpine:latest AS runner
RUN apk add --no-cache tzdata libgcc
RUN addgroup -S ergosphere && adduser -S ergosphere -G ergosphere

WORKDIR /app

COPY --from=builder /usr/src/ergosphere/target/release/ergosphere /usr/local/bin/ergosphere
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh && \
    mkdir -p /home/ergosphere/.config/ergosphere && \
    chown -R ergosphere:ergosphere /home/ergosphere /app

USER ergosphere

ENV RUST_LOG=info
ENV TZ=UTC

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh", "/usr/local/bin/ergosphere"]
CMD ["sync", "--daemon"]
