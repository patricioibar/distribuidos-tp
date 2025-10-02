# Multi-stage Dockerfile for Filter Service
# Stage 1: Build stage
FROM golang:1.25-alpine AS builder

# Set working directory to root to maintain relative paths
WORKDIR /build

# Install git (needed for go mod download)
RUN apk add --no-cache git

# Copy dependency modules (maintaining relative structure)
COPY innercommunication ./innercommunication
COPY middleware ./middleware

# Copy filter module
COPY filter ./filter

# Change to filter directory and build
WORKDIR /build/filter

# Download dependencies
RUN go mod download

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o filter .

# Stage 2: Runtime stage
FROM alpine:3.18

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates tzdata

# Create a non-root user
RUN addgroup -g 1001 -S filtergroup && \
    adduser -u 1001 -S filteruser -G filtergroup

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /build/filter/filter .

# Make binary executable
RUN chmod +x filter

# Change ownership to non-root user
RUN chown -R filteruser:filtergroup /app

# Switch to non-root user
USER filteruser

# Expose health check port (if needed in the future)
EXPOSE 8080

# Environment variables with default values
ENV FILTER_ID=""
ENV WORKERS_COUNT="1"
ENV FILTER_TYPE=""
ENV CONSUMER_NAME=""
ENV MW_ADDRESS="amqp://guest:guest@rabbitmq:5672/"
ENV SOURCE_QUEUE=""
ENV OUTPUT_EXCHANGE=""

# Health check (optional)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep filter || exit 1

# Set the binary as entrypoint
ENTRYPOINT ["./filter"]