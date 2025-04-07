FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY k3SQLServer/go.mod k3SQLServer/go.sum ./
COPY k3SQLServer/ ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o k3sql-server ./cmd/main.go

FROM alpine:latest

RUN mkdir -p /opt/k3SQL && \
    chmod 755 /opt/k3SQL

WORKDIR /app
COPY --from=builder /app/k3sql-server .

RUN chmod +x k3sql-server && \
    chown -R 1000:1000 /opt/k3SQL

EXPOSE 3003
CMD ["./k3sql-server"]