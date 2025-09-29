FROM golang:1.25 as builder

WORKDIR /src

COPY . .

RUN go env -w GO111MODULE=on

ENV CGO_ENABLED=0
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /usr/local/bin/aggregator ./aggregator/

FROM busybox:latest
COPY --from=builder /usr/local/bin/aggregator /usr/local/bin/aggregator

ENTRYPOINT ["/usr/local/bin/aggregator"]
