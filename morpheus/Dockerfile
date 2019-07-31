# ===============================================
# python build stage
FROM python:3.7-slim as build

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc linux-libc-dev libc6-dev

RUN pip install -U pip setuptools
ADD ./requirements.txt /home/root/requirements.txt
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

# ===============================================
# final image
FROM python:3.7-slim

COPY --from=build /usr/local/bin/atoolbox /usr/local/bin/
COPY --from=build /usr/lib/* /usr/lib/
COPY --from=build /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages

ADD ./app /home/root/app

WORKDIR /home/root

ARG COMMIT
ARG BUILD_TIME
ENV PYTHONUNBUFFERED=1 \
    ON_DOCKER=1 \
    ATOOLBOX_LOG_NAME=morpheus \
    ATOOLBOX_SETTINGS=app.settings.Settings \
    APP_COMMIT=$COMMIT \
    APP_BUILD_TIME=$BUILD_TIME

CMD ["atoolbox", "auto"]
