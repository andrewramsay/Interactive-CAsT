# syntax=docker/dockerfile:1.3
FROM common:latest

LABEL Author="Paul Owoicho"

COPY rewriter/requirements.txt /source/requirements.txt

RUN --mount=type=cache,mode=0755,target=/root/.cache/pip \
    pip3 install -r source/requirements.txt

COPY rewriter/ /source

WORKDIR /source

CMD python3 -u main.py
