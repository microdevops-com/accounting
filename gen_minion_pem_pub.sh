#!/bin/bash
cd /tmp
rm -f priv.key pub.key
openssl genrsa -out priv.key 2>/dev/null
openssl rsa -in priv.key -pubout -out pub.key 2>/dev/null

echo "    minion:"
echo "      pem: |"
sed -e "s/^/        /" /tmp/priv.key
echo "      pub: |"
sed -e "s/^/        /" /tmp/pub.key

rm -f priv.key pub.key
