#!/usr/bin/env bash

. env/bin/activate

export COMMIT=`git rev-parse HEAD`
export PS1="DEV $PS1"
