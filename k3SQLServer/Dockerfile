FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o server ./cmd/main

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .
EXPOSE 3003
CMD ["./server"]