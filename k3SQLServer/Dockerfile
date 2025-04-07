FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY k3SQLServer/go.mod k3SQLServer/go.sum ./
COPY k3SQLServer/ ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o k3sql-server ./cmd/main.go

FROM alpine:latest

RUN mkdir -p /opt/k3SQL/data
RUN mkdir -p /opt/k3SQL/config
RUN chmod 755 /opt/k3SQL

RUN mkdir -p /opt/k3SQL/data/k3db
RUN echo -e "3 user|3 password\nk3user|333" > /opt/k3SQL/data/k3db/users.k3

WORKDIR /app
COPY --from=builder /app/k3sql-server .

RUN chmod +x k3sql-server && \
    chown -R 1000:1000 /opt/k3SQL

EXPOSE 3003
CMD ["./k3sql-server"]