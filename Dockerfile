FROM golang:1.25.13-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tronwatch ./cmd/tronwatch

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tronwatch /usr/local/bin/tronwatch
ENTRYPOINT ["/usr/local/bin/tronwatch"]
CMD ["run", "--config", "/etc/tronwatch/config.json"]
