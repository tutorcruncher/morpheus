#!/usr/bin/env bash

status='ok'
if grep -Rn "^ *debug(" morpheus/; then
    status='failed'
fi

if grep -Rn "^ *debug(" tests/; then
    status='failed'
fi

if [ "$status" != "ok" ]; then
    echo "FAILED: debug commands found"
    exit 1
fi
