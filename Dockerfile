FROM golang:1.25.10-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY third_party/kubernetes ./third_party/kubernetes
RUN go mod download

COPY src ./src
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags='-s -w' \
    -o /out/batchscheduler ./src/cmd/batchscheduler
RUN mkdir -p /out/data

FROM scratch
COPY --from=build /out/batchscheduler /batchscheduler
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/data/ /var/lib/batchscheduler/
USER 65532:65532
ENTRYPOINT ["/batchscheduler"]
CMD ["--history-db=/var/lib/batchscheduler/history.db"]
