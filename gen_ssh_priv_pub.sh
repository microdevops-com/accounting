#!/bin/bash

if [[ -z $1 ]]; then
	echo ERROR: domain arg required
	exit 1
fi

KEYS_TMP_DIR=$(mktemp -d -t ssh-key.XXXXXXXXXX)
MYDIR=$(dirname $0)

ssh-keygen -N "" -t ed25519 -f ${KEYS_TMP_DIR}/key -C root@$1 > /dev/null 2>&1

cat ${KEYS_TMP_DIR}/key
echo
cat ${KEYS_TMP_DIR}/key.pub

rm -rf ${KEYS_TMP_DIR}
