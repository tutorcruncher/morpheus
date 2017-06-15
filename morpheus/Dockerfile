FROM python:3.6-alpine

RUN apk --update --no-cache add gcc g++ musl-dev zlib-dev libuv libffi-dev make \
 && rm -rf /var/cache/apk/*

# having a seperate file for packages which are slow to install speeds up some builts a lot
ADD ./requirements_one.txt /home/root/requirements_one.txt
RUN pip install -r /home/root/requirements_one.txt

ADD ./requirements_two.txt /home/root/requirements_two.txt
RUN pip install -r /home/root/requirements_two.txt

ADD ./app /home/root/app
ADD ./run.py /home/root/run.py

WORKDIR /home/root
#HEALTHCHECK --interval=60s --timeout=10s --retries=2 CMD /home/root/run.py check || exit 1
ENTRYPOINT ["./run.py"]
CMD ["web"]
