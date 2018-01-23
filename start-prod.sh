#!/usr/bin/env bash

gnome-terminal \
    --geometry=180x40 \
    --tab-with-profile=prod \
    -- bash -c "cd $(dirname "$0");
    exec bash --rcfile ${1:-activate.prod.sh}"
