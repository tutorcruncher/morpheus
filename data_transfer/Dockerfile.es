FROM docker.elastic.co/elasticsearch/elasticsearch:5.6.2
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install repository-s3
