FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY k3SQLServer/go.mod k3SQLServer/go.sum ./
COPY k3SQLServer/ ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o server ./cmd/main.go

FROM alpine:latest

RUN mkdir -p /opt/k3SQL && \
    chmod 755 /opt/k3SQL

RUN chmod +x k3sql-server && \
    chown -R 1000:1000 /opt/k3SQL

WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 3003
CMD ["./server"]