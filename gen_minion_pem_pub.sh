#!/bin/bash

KEYS_TMP_DIR=$(mktemp -d -t salt-key.XXXXXXXXXX)
MYDIR=$(dirname $0)

${MYDIR}/salt-key.py --gen-keys=${KEYS_TMP_DIR}/key > /dev/null 2>&1

echo "    minion:"
echo "      pem: |"
cat ${KEYS_TMP_DIR}/key.pem | sed -e "s/^/        /"
echo
echo "      pub: |"
cat ${KEYS_TMP_DIR}/key.pub | sed -e "s/^/        /"
echo

rm -rf ${KEYS_TMP_DIR}
