FROM ubuntu:latest

WORKDIR /usr/coindeck/chipmunkdb

ENV DEBIAN_FRONTEND noninteractive



RUN apt-get update && \
    apt-get upgrade -y

RUN apt-get install -y software-properties-common

RUN apt-add-repository -y universe
RUN apt-add-repository -y ppa:deadsnakes/ppa

RUN apt-get update

RUN apt-get install build-essential -y
RUN apt-get install wget curl -y

RUN apt-get install python3.10 -y

RUN apt-get install python3-pip -y
RUN apt-get install python3-dev -y

RUN apt -y install curl dirmngr apt-transport-https lsb-release ca-certificates

RUN apt-get install git -y


RUN pip3 install google
RUN pip3 install requests
RUN pip3 install sqlalchemy

#installing coindeck plugin
COPY chipmunkdb_server /usr/coindeck/chipmunkdb/chipmunkdb_server
COPY index.py /usr/coindeck/chipmunkdb/index.py
COPY requirements.txt /usr/coindeck/chipmunkdb/requirements.txt
COPY .env.docker /usr/coindeck/chipmunkdb/.env

WORKDIR /usr/coindeck/chipmunkdb/

RUN python3 --version
RUN pip3 install -r requirements.txt
COPY startup.sh /usr/coindeck/chipmunkdb/startup.sh

ENTRYPOINT ["bash", "/usr/coindeck/chipmunkdb/startup.sh"]

# test