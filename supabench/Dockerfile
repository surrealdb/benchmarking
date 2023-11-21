FROM golang:buster as builder

WORKDIR /builder

COPY ./go.mod ./go.sum ./
RUN go mod download

ADD . .

ENV CGO_ENABLED 0
RUN go build -o server

# release image
FROM gcr.io/google-appengine/debian10

COPY --from=builder /builder/server /

WORKDIR /

ENTRYPOINT ["/server", "serve", "--dir", "/pb_data", "--http", "0.0.0.0:8090"]
