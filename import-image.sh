#!/usr/bin/env bash

set -eo pipefail

IMAGE=$1
DEST_IMAGE_NAME="${2:-${IMAGE}}"

cleanup-container() {
  if [ "${CTR_ID}" ]; then
    docker rm -f "${CTR_ID}"
    unset CTR_ID
  fi
}
trap cleanup-container EXIT

CTR_ID=$(docker create "${IMAGE}")

docker export "${CTR_ID}" |
  time go run cmd/import_tar.go /dev/stdin "${DEST_IMAGE_NAME}"
  # time ./import_tar /dev/stdin "${DEST_IMAGE_NAME}"
cleanup-container
