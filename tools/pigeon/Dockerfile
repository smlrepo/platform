FROM golang:1.11.0 as builder
ENV PIGEON_REVISION 2ad051b8b508f69e7dcf8febdea2856ec2db73ed
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends unzip
RUN curl -s -o /pigeon.zip https://codeload.github.com/mna/pigeon/zip/${PIGEON_REVISION} && \
    unzip -q /pigeon.zip && \
    mkdir -p /go/src/github.com/mna && \
    mv pigeon-${PIGEON_REVISION} /go/src/github.com/mna/pigeon && \
    # The only dependencies this package has are in golang.org/x/tools so it is safe
    # not to pin them to anything and upstream doesn't pin anything.
    go get github.com/mna/pigeon && \
    go build -o /go/bin/pigeon github.com/mna/pigeon && \
    rm -f /pigeon.zip

FROM debian:stretch-slim
COPY --from=builder /go/bin/pigeon /usr/bin/pigeon
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
