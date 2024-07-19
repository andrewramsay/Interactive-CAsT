FROM python:3.10.10-slim

LABEL Author="Paul Owoicho"

COPY builder/ /source

RUN pip3 install --upgrade pip
RUN pip3 install -r /source/requirements.txt

WORKDIR /source

CMD bash build_protos.sh
