# Stage 1: Build the Go application
FROM golang:1.20 AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY . .
# Build a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o chord main.go rpc.go utils.go node.go health.go conn.go

# Stage 2: Create the runtime image using Alpine
FROM alpine:3.17 AS runtime

WORKDIR /app
COPY --from=builder /app/chord /app/chord
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh /app/chord

# Set environment if needed
ENV IPADDRESS=0.0.0.0
ENV PORT=8000

EXPOSE 8000
ENTRYPOINT ["/app/entrypoint.sh"]