# How morpheus sends emails/SMS and records that 

### 1 send request comes in

POST request with msgpack body sign by shared key.

data consists of two parts: shared information about all the emails to send, details
on each individual email.

Shared info contains:
* group id
* outer template S3 path (email style)
* inner template S3 path (email definition)
* subject template
* context
* company code
* from address
* reply to address
* send method: sms, mandrill, ses, some kind of "test" mode.
* (optionally) mandrill sub-account
* tags - ways to reference all these message (eg. email definition)

Individual message info contains:
* user's first and last name 
* destination address
* tags - ways to reference this message (eg. user id invoice id)
* context
* PDF attachment html
* and/or S3 path for attachments

### 2 message auth

Message is authenticated based on header value.

### 3 messages into list

The individual message info is encoded into separate msgpack blobs for each message and thrown into a redis list.
 
### 4 job is enqueued

The job to send the emails is enqueued as an `arq` job with the shared info.

### 5 job starts

The job main job starts, templates are pulled from S3. How do we use shared memory for the templates?

Info about the "message group" is saved to elasticsearch to become the parent in a parent-child relationship. 

New jobs are created to send each message.

### 6 each message is sent

Perhaps do message and PDF rendering (which are CPU bound and require the templates) in the main job.

* message rendered from context
* attachments either pulled from S3 or PDF generated from html
* message sent to sending service
* message body, subject, context, tags, sending response (with SMS this needs to include cost of send) 
are added to elasticsearch under `/message/{company-code}`, es will create the id.

# Notes

arq should take care of re-enqueueing the main jobs if the worker shuts down, see samuelcolvin/arq#38.

Webhooks from the sending service update the elasticsearch documents for each message.

upstream apps can get data about sent messages:
* aggregate data on all sent messages, delivery rates etc.
* check how a job/"send group" is getting on
* searchable list of sent messages
* filtered list of messages based on `group_id` or tags (eg. invoice number)
* cost of messages sent in a given period
