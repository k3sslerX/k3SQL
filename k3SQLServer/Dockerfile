FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o k3sql-server ./k3SQLServer

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/k3sql-server .
RUN chmod +x k3sql-server
EXPOSE 3003
CMD ["./k3sql-server", "0.0.0.0", "3003"]