FROM python:3.6-alpine

RUN apk --update --no-cache add gcc g++ musl-dev zlib-dev libuv libffi-dev openssl-dev make \
 && rm -rf /var/cache/apk/*

# installing these locally means minor changes to requirements should slow donwn build too much
RUN pip install cffi cryptography multidict uvloop

ADD ./requirements.txt /home/root/requirements.txt
RUN pip install -r /home/root/requirements.txt

ADD ./app /home/root/app
ADD ./run.py /home/root/run.py

WORKDIR /home/root
#HEALTHCHECK --interval=60s --timeout=10s --retries=2 CMD /home/root/run.py check || exit 1
ENTRYPOINT ["./run.py"]
CMD ["web"]
