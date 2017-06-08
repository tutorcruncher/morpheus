FROM nginx:1.13-alpine
ADD ./allowed.nginx.conf /etc/nginx/allowed.nginx.conf
ADD ./keys/cert.pem /ssl/cert.pem
ADD ./keys/key.pem /ssl/key.pem
ADD ./htpasswd /htpasswd
ADD ./nginx.conf /etc/nginx/nginx.conf
