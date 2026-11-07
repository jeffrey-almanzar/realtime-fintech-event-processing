# Builds any one service: docker build --build-arg SERVICE=api .
# Valid values are the directories in src/cmd.
FROM golang:1.27-bookworm AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY src ./src
# confluent-kafka-go links librdkafka statically but needs cgo and glibc.
RUN go build -trimpath -ldflags "-s -w" -o /app "./src/cmd/${SERVICE}"

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /app /app
ENTRYPOINT ["/app"]
