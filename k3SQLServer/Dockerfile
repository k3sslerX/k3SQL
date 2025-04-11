FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY k3SQLServer/go.mod k3SQLServer/go.sum ./
COPY k3SQLServer/ ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o k3sql-server ./main.go

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/k3sql-server .

RUN chmod +x k3sql-server && \
    chown -R 1000:1000 /opt/k3SQL

EXPOSE 3003
CMD ["sudo ./k3sql-server", "0.0.0.0", "3003"]