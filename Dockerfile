FROM golang:1.22-alpine AS builder

LABEL stage=gobuilder \
      mainatiner=https://github.com/xgzlucario/rotom

ENV CGO_ENABLED 0
ENV GOPROXY https://goproxy.cn,direct

ARG BUILD_TIME

WORKDIR /build

COPY . .

RUN go build -ldflags="-s -w -X main.buildTime=${BUILD_TIME}" -o rotom .

FROM alpine:latest

ENV TZ Asia/Shanghai
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apk/repositories
RUN apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates

RUN apk --no-cache add redis bash

VOLUME /data
WORKDIR /data

COPY --from=builder /build/rotom /data/rotom
COPY config.json /etc/rotom/config.json

EXPOSE 6379

CMD ["./rotom", "-config", "/etc/rotom/config.json"]