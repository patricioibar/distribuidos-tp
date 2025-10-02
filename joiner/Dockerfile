FROM golang:1.25 AS builder

WORKDIR /src

COPY . .

RUN go env -w GO111MODULE=on

ENV CGO_ENABLED=0
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /usr/local/bin/joiner ./joiner/

FROM busybox:latest
COPY --from=builder /usr/local/bin/joiner /usr/local/bin/joiner

ENTRYPOINT ["/usr/local/bin/joiner"]
