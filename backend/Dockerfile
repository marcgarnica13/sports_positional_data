FROM python:3.6.8

LABEL MAINTAINER="Marc Garnica CAparros <m.garnica@dshs-koeln.de>"

WORKDIR /var/backend/

ADD . /var/backend/

RUN apt-get update
RUN apt-get install python python-pip build-essential cmake pkg-config libx11-dev libatlas-base-dev libgtk-3-dev libboost-python-dev -y
RUN pip install --upgrade pip
RUN python3 -m pip install --user --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "/var/backend/run.py"]
