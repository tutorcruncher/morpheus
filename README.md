# Morpheus

[![Build Status](https://travis-ci.org/tutorcruncher/morpheus.svg?branch=master)](https://travis-ci.org/tutorcruncher/morpheus)
[![codecov.io](https://codecov.io/gh/tutorcruncher/morpheus/branch/master/graph/badge.svg)](https://codecov.io/gh/tutorcruncher/morpheus)

MIT license, Copyright (c) 2017 TutorCruncher & Samuel Colvin. See [LICENSE](LICENSE) for details.

"The Greek God of Dreams who delivered messages from the gods to the mortal world"

Okay, chill. We're not normally that astronomically arrogant. Just the obvious mythological name for a messaging
platform - "hermes" was already taken by a plethora of terrible nineties mail clients.

**work in progress** - not yet ready for production.

What will *morpheus* do?
* sends emails and SMSs fast using mandrill, SES and message bird. One http request to send 5000 emails.
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

    docker-compose up -d --build
    
`--build` makes sure to build any changes to the morpheus image, `-d` is detach

At the same time in another window

    until $(curl -so /dev/null http://localhost:8001/logs -I && true); do printf .; sleep 0.1; done && curl -s http://localhost:8001/logs
    
To view the logs

You can also run either the web or worker with

    ./morpheus/run.py web
    # OR
    ./morpheus/run.py worker

You'll need elastic search and redis installed.

### To deploy



Set up your environment

    source activate.sh

(this assumes you have a `prod` gnome profile setup to differentiate commands going to the production server)

then

    ./deploy/deploy

That same command should also work to update the deployment after a change.
