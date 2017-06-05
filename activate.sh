#!/usr/bin/env bash

. env/bin/activate
export APP_AUTH_KEY='testing'
export APP_MANDRILL_KEY='invalid'
export APP_USER_FERNET_KEY='i am not secure but 32 bits long'

export COMMIT=`git rev-parse HEAD`
export RELEASE_DATE='-'
export SERVER_NAME='localhost'
export RAVEN_DSN=''

export PS1="DEV $PS1"
