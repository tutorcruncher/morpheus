#!/usr/bin/env bash

. env/bin/activate
export APP_AUTH_KEY='testing'
export APP_MANDRILL_KEY='invalid'

export PS1="DEV $PS1"
