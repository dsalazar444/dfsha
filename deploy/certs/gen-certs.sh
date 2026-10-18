#!/bin/bash

# Genera la CA de DFSha y un certificado por nodo, firmado por esa CA
# Cada argumento es NODO=NOMBRE[,NOMBRE...]. Los nombres son los que usarán los demás para llamar a ese nodo:
# un nombre de servicio (sp1), un DNS o una IP (la IP elástica en AWS). Quedan en el certificado, y la
# conexión HTTPS solo funciona si el nombre con que se llama al nodo está en esa lista

# Salida, en deploy/certs/out/ 
#   ca.crt            certificado de la CA: lo reparten TODOS los nodos y el CLI para confiar en los demás
#   ca.key            clave de la CA: es lo más delicado, con ella se fabrican certificados válidos. NO se reparte
#   <nodo>.crt/.key   certificado y clave de cada nodo: cada nodo recibe solo los suyos

# Si ya existe una CA se reutiliza, así los certificados que ya se repartieron siguen valiendo.

set -euo pipefail

# Git Bash en Windows convierte "/CN=..." en una ruta de archivo y rompe el comando
export MSYS_NO_PATHCONV=1

# CERTS_DIR cambia la carpeta de salida, por ejemplo para los certificados de AWS sin pisar los locales:
#   CERTS_DIR=deploy/certs/out-aws bash deploy/certs/gen-certs.sh sp1=...
OUT="${CERTS_DIR:-$(cd "$(dirname "$0")" && pwd)/out}"
mkdir -p "$OUT" && OUT="$(cd "$OUT" && pwd)"   # ruta absoluta, venga como venga CERTS_DIR
CA_DAYS=3650    
NODE_DAYS=825   

# Sin argumentos: los nodos del compose local. "localhost" y 127.0.0.1 permiten probar desde el computador
NODES=("$@")
if [ ${#NODES[@]} -eq 0 ]; then
  NODES=(
    "sp1=sp1,localhost,127.0.0.1"
    "sp2=sp2,localhost,127.0.0.1"
    "peer1=peer1,localhost,127.0.0.1"
    "peer2=peer2,localhost,127.0.0.1"
    "peer3=peer3,localhost,127.0.0.1"
  )
fi

mkdir -p "$OUT"
cd "$OUT"

# ---------- la CA ----------
if [ -f "ca.key" ] && [ -f "ca.crt" ]; then
  echo "CA existente: se reutiliza ($OUT/ca.crt)"
else
  echo "Creando la CA"
  openssl ecparam -name prime256v1 -genkey -noout -out "ca.key"
  openssl req -x509 -new -key "ca.key" -sha256 -days "$CA_DAYS" -subj "/CN=DFSha CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "ca.crt"
  chmod 600 "ca.key"
fi

# ---------- un certificado por nodo ----------
for spec in "${NODES[@]}"; do
  node="${spec%%=*}"      # lo que va antes del primer "="
  names="${spec#*=}"      # lo que va después
  if [ "$node" = "$spec" ] || [ -z "$names" ]; then
    echo "Argumento inválido '$spec': use NODO=NOMBRE[,NOMBRE...]"; exit 1
  fi

  # subjectAltName -> cada nombre va como IP: o como DNS: según tenga forma de dirección IP
  san=""
  IFS=',' read -ra list <<< "$names"
  for name in "${list[@]}"; do
    if [[ "$name" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then kind="IP"; else kind="DNS"; fi
    san="${san:+$san,}$kind:$name"
  done

  echo "Certificado de $node  ($san)"
  openssl ecparam -name prime256v1 -genkey -noout -out "$node.key"
  openssl req -new -key "$node.key" -subj "/CN=$node" -out "$node.csr"
  printf 'subjectAltName=%s\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=serverAuth\n' "$san" > "$node.ext"
  openssl x509 -req -in "$node.csr" -CA "ca.crt" -CAkey "ca.key" -CAcreateserial \
    -sha256 -days "$NODE_DAYS" -extfile "$node.ext" -out "$node.crt" 2> /dev/null
  rm -f "$node.csr" "$node.ext"

  # La clave del nodo la lee el contenedor (usuario 10001); la clave de la CA sí queda en 600
  chmod 644 "$node.key"

  # Comprobación: el certificado debe estar firmado por la CA
  openssl verify -CAfile "ca.crt" "$node.crt" > /dev/null
done

echo "Listo: $OUT"
