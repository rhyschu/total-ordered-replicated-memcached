FROM golang:1.27.3 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /rpcd ./cmd/rpcd
RUN mkdir /state
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /rpcd /rpcd
COPY --from=build --chown=65532:65532 /state /data
WORKDIR /data
ENTRYPOINT ["/rpcd"]
