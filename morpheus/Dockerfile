# ===============================================
# python build stage
FROM python:3.7-slim as build

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc python-dev

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

#COPY --from=build /lib/* /lib/
COPY --from=build /usr/lib/* /usr/lib/
COPY --from=build /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages

ADD ./app /home/root/app
ADD ./run.py /home/root/run.py

WORKDIR /home/root
#HEALTHCHECK --interval=60s --timeout=10s --retries=2 CMD /home/root/run.py check || exit 1
ENTRYPOINT ["./run.py"]
CMD ["web"]
