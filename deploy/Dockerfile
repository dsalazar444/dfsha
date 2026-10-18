# syntax=docker/dockerfile:1
#
# Imagen de un componente de DFSha. Un solo Dockerfile sirve para todos:
# el argumento CMD dice cuál de los programas de cmd/ se compila
#
#   docker build -f deploy/Dockerfile --build-arg CMD=superpeer -t dfsha-superpeer .
#   docker build -f deploy/Dockerfile --build-arg CMD=peer      -t dfsha-peer .     (cuando exista cmd/peer)
#
# El contexto de build es la raíz del repo (el punto final)

# ---- Etapa 1: compilar ----
# La versión de Go debe ser igual o mayor a la de go.mod
FROM golang:1.27-alpine AS build
ARG CMD=superpeer
WORKDIR /src

# Primero solo las dependencias: mientras go.mod y go.sum no cambien, Docker reutiliza esta capa
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 da un binario estático, que corre en la imagen mínima de abajo
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# ---- Etapa 2: imagen final, pequeña y sin el compilador ----
# alpine (y no una imagen vacía) porque trae wget, que usa el healthcheck del compose
FROM alpine:3.20

# No se corre como root. /data es la carpeta del estado (STATE_DIR): se crea aquí con su dueño para que,
# al montar un volumen nuevo encima, Docker lo copie con esos permisos
RUN adduser -D -u 10001 app && mkdir /data && chown app:app /data
USER app

COPY --from=build /out/app /app

ENV STATE_DIR=/data
EXPOSE 8080
ENTRYPOINT ["/app"]
