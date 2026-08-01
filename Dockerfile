FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/broker-bin ./cmd/server/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/broker-bin .

RUN mkdir -p app/data

EXPOSE 8090
EXPOSE 2112

ENTRYPOINT [ "/app/broker-bin" ]