FROM mirror.gcr.io/golang:alpine AS builder
WORKDIR /build
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o mock-prometheus-server .

FROM mirror.gcr.io/alpine:3
RUN addgroup -S app && adduser -S -G app app
COPY --from=builder /build/mock-prometheus-server /usr/local/bin/mock-prometheus-server
USER app
EXPOSE 9090
ENTRYPOINT ["mock-prometheus-server"]
