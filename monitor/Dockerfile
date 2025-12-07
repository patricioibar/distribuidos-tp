FROM golang:1.25 AS builder

WORKDIR /src

COPY . .

RUN go env -w GO111MODULE=on

ENV CGO_ENABLED=0
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /usr/local/bin/monitor ./monitor/

FROM alpine:3.20
RUN apk add --no-cache docker-cli
COPY --from=builder /usr/local/bin/monitor /usr/local/bin/monitor

ENTRYPOINT ["/usr/local/bin/monitor"]
