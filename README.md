# Morpheus

[![Build Status](https://travis-ci.org/tutorcruncher/morpheus.svg?branch=master)](https://travis-ci.org/tutorcruncher/morpheus)
[![codecov.io](https://codecov.io/gh/tutorcruncher/morpheus/branch/master/graph/badge.svg)](https://codecov.io/gh/tutorcruncher/morpheus)
[![pypi](https://img.shields.io/pypi/v/morpheus-mail.svg)](https://pypi.python.org/pypi/morpheus-mail)

MIT license, Copyright (c) 2021 TutorCruncher & Samuel Colvin. See [LICENSE](LICENSE) for details.

"The Greek God of Dreams who delivered messages from the gods to the mortal world"

Okay, chill. We're not normally that astronomically arrogant. Just the obvious mythological name for a messaging
platform - "hermes" was already taken by a plethora of terrible nineties mail clients.

What *morpheus* does:
* sends emails and SMSs fast using [Mandrill](https://www.mandrillapp.com/docs/), SES (TODO) and [Messagebird](https://www.messagebird.com/en/) (SMS). One http request to send 5000 emails or SMSs.
* generate PDFs for attachments given HTML using wkhtmltopdf.
* provide a searchable history of sent messages and delivery analytics.

Here's a picture to help explain:

![Morpheus and Iris](https://raw.githubusercontent.com/tutorcruncher/morpheus/master/morpheus.png)

## Usage

Morpheus is built with [FastAPI](https://fastapi.tiangolo.com/), using [asyncpg](https://github.com/MagicStack/asyncpg) 
to access a Postgres database and some really handy tools from [foxglove](https://github.com/samuelcolvin/foxglove).

### Running locally

Set up your environment, run `make install`, create your database with `make reset-db` and you're ready to go.

You can run the web worker with

```
foxglove web
```

and the worker in a different terminal with

```
foxglove worker
```

You'll need postgres and redis installed.
