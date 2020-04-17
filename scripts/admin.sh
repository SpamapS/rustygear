#!/usr/bin/env bash
set -eux

response=$(echo "version" | nc localhost 4730)
if [[ "$response" = "OK rustygear-version-here" ]] ; then
    echo OK!
else
    echo FAIL!
    exit 1
fi
