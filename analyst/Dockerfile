FROM golang:1.25-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git

COPY communication ./communication
COPY analyst ./analyst

WORKDIR /build/analyst
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o analyst .

FROM alpine:3.18

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /build/analyst/analyst .
RUN chmod +x analyst

ENTRYPOINT ["./analyst"]