#!/usr/bin/env bash
# run elasticsearch in docker. Useful for tests or when running morpheus locally without compose

docker run --rm \
  -p 9200:9200 \
  -e "http.host=0.0.0.0" \
  -e "transport.host=127.0.0.1" \
  -e "xpack.security.enabled=false" \
  -e "path.repo=[\"/snapshots\"]" \
  docker.elastic.co/elasticsearch/elasticsearch:5.6.2
