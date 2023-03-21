FROM python:3.10.10-slim

RUN pip3 install --upgrade pip

COPY web_ui/requirements.txt /source/requirements.txt

RUN pip3 install -r source/requirements.txt

COPY web_ui/ /source

WORKDIR /source

CMD python3 -u main.py
