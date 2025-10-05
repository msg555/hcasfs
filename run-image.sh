#!/usr/bin/env bash

set -eo pipefail
set -x

ROOT_OBJECT=$1
shift

unmount() {
  if [ "${CONTAINER_DIR}" ]; then
    sudo umount "${CONTAINER_DIR}/rootfs" || true
    sudo umount "${CONTAINER_DIR}/lower" || true
  fi
}
trap unmount EXIT

CONTAINER_DIR=$(mktemp -d)
mkdir "${CONTAINER_DIR}/"{rootfs,lower,upper,work}

sudo mount -t hcasfs /hcas-data/ "${CONTAINER_DIR}/lower" -o "root_object=${ROOT_OBJECT}"

cd "${CONTAINER_DIR}"

sudo mount -t overlay overlay -o "lowerdir=${PWD}/lower,upperdir=${PWD}/upper,workdir=${PWD}/work" rootfs/

runc spec

ARGS=$(printf '%s\n' "$@" | jq -R . | jq -s .)

CAPS='["CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FSETID","CAP_FOWNER","CAP_MKNOD","CAP_NET_RAW","CAP_SETGID","CAP_SETUID","CAP_SETFCAP","CAP_SETPCAP","CAP_NET_BIND_SERVICE","CAP_SYS_CHROOT","CAP_KILL","CAP_AUDIT_WRITE"]'
jq ".process.args = ${ARGS}" config.json > config.json.tmp
jq ".root.readonly = false" config.json.tmp > config.json
jq ".process.capabilities.bounding = ${CAPS}" config.json > config.json.tmp
jq ".process.capabilities.effective = ${CAPS}" config.json.tmp > config.json
jq ".process.capabilities.permitted = ${CAPS}" config.json > config.json.tmp
jq '(.linux.namespaces) |= map(select(.type != "network"))' config.json.tmp > config.json
# mv config.json.tmp config.json

sudo cp /etc/resolv.conf rootfs/etc/resolv.conf

sudo runc run test-container
