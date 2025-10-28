FROM golang:1.24 AS builder

RUN mkdir /app

COPY . /app

WORKDIR /app

RUN CGO_ENABLED=0 go build -o nodeApp ./cmd

RUN chmod +x /app/nodeApp


FROM alpine:latest
RUN mkdir /app

COPY --from=builder /app/nodeApp /app
COPY --from=builder /app/node/config.json ./node/config.json

CMD ["./app/nodeApp"]
