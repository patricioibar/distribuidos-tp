FROM golang:1.25-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git

COPY communication ./communication
COPY innercommunication ./innercommunication
COPY middleware ./middleware
COPY coffee-analyzer ./coffee-analyzer

WORKDIR /build/coffee-analyzer
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o coffee-analyzer .

FROM alpine:3.18

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /build/coffee-analyzer/coffee-analyzer .
RUN chmod +x coffee-analyzer

ENTRYPOINT ["./coffee-analyzer"]