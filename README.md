# Morpheus

[![Build Status](https://travis-ci.org/tutorcruncher/morpheus.svg?branch=master)](https://travis-ci.org/tutorcruncher/morpheus)
[![codecov.io](https://codecov.io/gh/tutorcruncher/morpheus/branch/master/graph/badge.svg)](https://codecov.io/gh/tutorcruncher/morpheus)

Copyright (c) 2017 TutorCruncher, Samuel Colvin. See LICENSE for details.

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

![Morpheus and Iris](https://raw.githubusercontent.com/samuelcolvin/files/master/morpheus.png)
