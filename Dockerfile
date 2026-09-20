# Serviço de carteiras — imagem da aplicação.
#
# Declaração da versão do Go em um único ponto (ARG), mantida em sincronia com
# go.mod (Go 1.25). Atualize ambos ao subir a versão do toolchain.
#
# Multi-stage: etapa de build compila o binário estático; a etapa final é a
# imagem de execução mínima (alpine) com o binário e sem tooling.
ARG GO_VERSION=1.25

# --- Etapa de build ---
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# Copia somente go.mod/go.sum para aproveitar cache de camadas de dependências.
COPY go.mod go.sum ./
RUN go mod download

# Copia o restante e compila o binário estático.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/wallet-service ./cmd/server && \
    go build -o /out/migrate ./cmd/migrate

# --- Etapa final (runtime mínima) ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 10001 wallet

COPY --from=build /out/wallet-service /usr/local/bin/wallet-service
COPY --from=build /out/migrate /usr/local/bin/wallet-migrate

USER wallet
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/wallet-service"]