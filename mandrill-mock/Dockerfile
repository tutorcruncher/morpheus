# ===============================================
# python build stage
FROM python:3.9-slim as build

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc linux-libc-dev libc6-dev

RUN pip install -U pip setuptools
RUN pip install aiohttp==3.5.4

# get rid of unnecessary files to keep the size of site-packages and the final image down
RUN find /usr \
    -name '*.py[codxi]' -o \
    -name '*.pxd' -o \
    -name '*.c' -o \
    -name '*.h' -o \
    -name '*.txt' | xargs rm
RUN find /usr -name '__pycache__' -delete
RUN find /usr -name '*.dist-info'  | xargs rm -r

# ===============================================
# final image
FROM python:3.9-slim

COPY --from=build /usr/lib/* /usr/lib/
COPY --from=build /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

ADD ./server.py /home/root/server.py

WORKDIR /home/root

ENV PYTHONUNBUFFERED=1

CMD ["python", "server.py"]
