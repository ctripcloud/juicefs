#!/bin/sh
set -e

# Check if TIKV_ADDR is set
if [ -z "$TIKV_ADDR" ]; then
    echo "Error: TIKV_ADDR environment variable is not set"
    sleep 10
    exit 1
fi
# if not set TiKV_METRICS, use default metrics address
if [ -z "$TIKV_METRICS" ]; then
    TIKV_METRICS="127.0.0.1:33900"
fi

# Use tikv-proxy with all arguments
if [ -z "$TIKV_DISCOVERY" ]; then
    exec /tini -g -- /usr/local/bin/juicefs tikv-proxy "$TIKV_ADDR"  --metrics "$TIKV_METRICS"
else
    exec /tini -g -- /usr/local/bin/juicefs proxy-discovery "$TIKV_ADDR"  --metrics "$TIKV_METRICS"
fi
