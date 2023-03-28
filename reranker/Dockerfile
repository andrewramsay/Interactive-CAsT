# syntax=docker/dockerfile:1.3
FROM common:latest

LABEL Author="Paul Owoicho"

COPY reranker/requirements.txt /source/requirements.txt

COPY reranker/rerankers/pygaggle/requirements.txt /source/rerankers/pygaggle/requirements.txt

RUN --mount=type=cache,mode=0755,target=/root/.cache/pip \
    pip3 install -r source/requirements.txt -r source/rerankers/pygaggle/requirements.txt

COPY reranker/ /source

WORKDIR /source

CMD python3 -u main.py
