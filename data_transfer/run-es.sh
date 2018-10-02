#!/usr/bin/env bash
# run elasticsearch in docker. Useful for tests or when running morpheus locally without compose

# "docker build . -f Dockerfile.es -t elasticsearch_s3" to create the image, also needs mkdir es_data
docker run --rm \
  -p 9200:9200 \
  -e "http.host=0.0.0.0" \
  -e "transport.host=127.0.0.1" \
  -e "xpack.security.enabled=false" \
  -v `pwd`/es_data:/usr/share/elasticsearch/data \
  elasticsearch_s3
