FROM python:3.9-alpine3.14

RUN apk update && apk add python3-dev \
                        gcc \
                        libc-dev

WORKDIR /app

COPY requirements.txt /

COPY test-requirements.txt /

RUN pip install -r /test-requirements.txt

COPY strechy /

COPY tests /tests