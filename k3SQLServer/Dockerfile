FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/k3sql-server ./k3SQLServer

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/k3sql-server /app/k3sql-server
RUN chmod +x /app/k3sql-server  # <-- Добавьте эту строку
EXPOSE 3003
CMD ["/app/k3sql-server", "0.0.0.0", "3003"]