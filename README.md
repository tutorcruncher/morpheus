# Morpheus

[![Build Status](https://travis-ci.org/tutorcruncher/morpheus.svg?branch=master)](https://travis-ci.org/tutorcruncher/morpheus)
[![codecov.io](https://codecov.io/gh/tutorcruncher/morpheus/branch/master/graph/badge.svg)](https://codecov.io/gh/tutorcruncher/morpheus)
[![pypi](https://img.shields.io/pypi/v/morpheus-mail.svg)](https://pypi.python.org/pypi/morpheus-mail)

MIT license, Copyright (c) 2017 TutorCruncher & Samuel Colvin. See [LICENSE](LICENSE) for details.

"The Greek God of Dreams who delivered messages from the gods to the mortal world"

Okay, chill. We're not normally that astronomically arrogant. Just the obvious mythological name for a messaging
platform - "hermes" was already taken by a plethora of terrible nineties mail clients.

What *morpheus* does:
* sends emails and SMSs fast using mandrill, SES (TODO) and messagebird (SMS). One http request to send 5000 emails or SMSs.
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

Get ssl `cert.pem` and `key.pem` and put them in `./nginx/keys`.

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

### Setting up the machine

you'll need to add

```
vm.max_map_count=262144
```

To the end of `/etc/sysctl.conf` to allow elastic search to boot.

### To deploy

Set up your environment

    ./start-prod.sh

(this assumes you have a `prod` gnome profile setup to differentiate commands going to the production server)

then

    ./deploy/deploy

That same command should also work to update the deployment after a change.


### To test

Set up your environment. If you have ElasticSearch installed and running you're fine, or you can run it with:

    ./run-es.sh

then

    make

### to monitor

backup in progress (the pydf image has curl installed)

    docker exec -it morpheus_pdf_1 curl elastic:9200/_cat/recovery?v

indices
    
    docker exec -it morpheus_pdf_1 curl elastic:9200/_cat/indices/?v


### to backup redis

```
docker exec -it morpheus_redis_1 redis-cli SAVE
docker cp morpheus_redis_1:/data/dump.rdb dump.rdb
```

### to restore redis

```
./deploy/compose stop redis
docker cp dump.rdb morpheus_redis_1:/data/dump.rdb
./deploy/compose start redis
```

check with

```
docker exec -it morpheus_redis_1 redis-cli DBSIZE
docker exec -it morpheus_redis_1 redis-cli INFO
```
