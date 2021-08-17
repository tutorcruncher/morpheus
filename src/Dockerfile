FROM python:3.9-slim

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc linux-libc-dev libc6-dev libffi-dev

WORKDIR /home/root

RUN pip install -U pip setuptools
ADD requirements.txt /home/root/requirements.txt
RUN pip install -r /home/root/requirements.txt

# get rid of unnecessary files to keep the size of site-packages and the final image down
RUN find /usr \
    -name '*.py[codxi]' -o \
    -name '*.pxd' -o \
    -name '*.c' -o \
    -name '*.h' -o \
    -name '*.txt' | xargs rm
RUN find /usr -name '__pycache__' -delete
RUN find /usr -name '*.dist-info'  | xargs rm -r

ADD ../src /home/root/

CMD ["foxglove", "auto"]
