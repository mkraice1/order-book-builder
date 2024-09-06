FROM ghcr.io/deephaven/server:latest

# copy python requirements and data
COPY requirements.txt /requirements.txt

RUN mkdir -p /apps/libs
COPY build/libs/*.jar /apps/libs/

# install python requirements
RUN pip install -r /requirements.txt && rm /requirements.txt
