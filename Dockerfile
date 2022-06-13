FROM golang:1.18-alpine

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
COPY tasq-server/*.go ./
RUN go mod download

EXPOSE 8080
ENTRYPOINT ["go", "run", "."]
CMD ["-addr=:8080"]