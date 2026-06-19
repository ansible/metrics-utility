FROM golang:alpine AS builder
WORKDIR /build
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -o mock-segment-server .

FROM alpine:3
RUN addgroup -S app && adduser -S -G app app
COPY --from=builder /build/mock-segment-server /usr/local/bin/mock-segment-server
USER app
EXPOSE 8765
ENTRYPOINT ["mock-segment-server"]
