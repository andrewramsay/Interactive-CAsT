FROM python:3.10.10-slim

LABEL Author="Paul Owoicho"

RUN pip3 install --upgrade pip
RUN pip3 install grpcio grpcio_tools protobuf==3.19.0

COPY builder/ /source

WORKDIR /source

CMD bash build_protos.sh
