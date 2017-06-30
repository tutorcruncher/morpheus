# Morpheus

[![Build Status](https://travis-ci.org/tutorcruncher/morpheus.svg?branch=master)](https://travis-ci.org/tutorcruncher/morpheus)
[![codecov.io](https://codecov.io/gh/tutorcruncher/morpheus/branch/master/graph/badge.svg)](https://codecov.io/gh/tutorcruncher/morpheus)
[![pypi](https://img.shields.io/pypi/v/morpheus-mail.svg)](https://pypi.python.org/pypi/morpheus-mail)

MIT license, Copyright (c) 2017 TutorCruncher & Samuel Colvin. See [LICENSE](LICENSE) for details.

"The Greek God of Dreams who delivered messages from the gods to the mortal world"

Okay, chill. We're not normally that astronomically arrogant. Just the obvious mythological name for a messaging
platform - "hermes" was already taken by a plethora of terrible nineties mail clients.

What *morpheus* does:
* sends emails and SMSs fast using mandrill, SES (TODO) and message bird. One http request to send 5000 emails.
* generate PDFs for attachments given HTML using wkhtmltopdf.
* provide a searchable history of sent messages and delivery analytics.
* manage sending quotas as mandrill does when not using mandrill.

Here's a picture to help explain:

![Morpheus and Iris](https://raw.githubusercontent.com/tutorcruncher/morpheus/master/morpheus.png)

## Usage

### Running locally

Set up your environment

    source activate.sh

then

    docker build morpheus -t morpheus && docker-compose up -d
    
`--build` makes sure to build any changes to the morpheus image, `-d` is detach

At the same time in another window

    until $(curl -so /dev/null http://localhost:8001/logs -I && true); do printf .; sleep 0.1; done && curl -s http://localhost:8001/logs
    
To view the logs

You can also run either the web or worker with

    ./morpheus/run.py web
    # OR
    ./morpheus/run.py worker

You'll need elastic search and redis installed.

### To prepare for deploy

Get ssl `cert.pem and `key.pem` and put htem in `./nginx/prod/keys`, generate a password file for glances

    sudo apt install apache2-utils
    sudo htpasswd -c nginx/prod/pword <username>

Create `activate.prod.sh`:

```shell
#!/usr/bin/env bash
. env/bin/activate
export SCALEWAY_ORGANIZATION='...1'
export SCALEWAY_TOKEN='...'
export LOGSPOUT_ENDPOINT='...'
export RAVEN_DSN='...'
export APP_AUTH_KEY='...'
export APP_MANDRILL_KEY='...'
export APP_USER_AUTH_KEY='...'
export APP_HOST_NAME='...'
export APP_PUBLIC_LOCAL_API_URL='...'
export APP_ADMIN_BASIC_AUTH_PASSWORD='...'
export APP_S3_ACCESS_KEY='...'
export APP_S3_SECRET_KEY='...'

export APP_MESSAGEBIRD_KEY='...'
export APP_MESSAGEBIRD_PRICING_USERNAME='...'
export APP_MESSAGEBIRD_PRICING_PASSWORD='...'

echo "enabling docker machine..."
eval $(docker-machine env morpheus)

export MODE='PRODUCTION'
export PS1="PROD $PS1"
```

### To deploy

Set up your environment

    ./start-prod.sh

(this assumes you have a `prod` gnome profile setup to differentiate commands going to the production server)

then

    ./deploy/deploy

That same command should also work to update the deployment after a change.
