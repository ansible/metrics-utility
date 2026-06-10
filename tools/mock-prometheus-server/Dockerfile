FROM golang:alpine AS builder
WORKDIR /build
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o mock-prometheus-server .

FROM alpine:3
COPY --from=builder /build/mock-prometheus-server /usr/local/bin/mock-prometheus-server
EXPOSE 9090
ENTRYPOINT ["mock-prometheus-server"]
