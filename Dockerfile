FROM saiqi/16mb-platform:latest

RUN mkdir /service 

ADD application /service/application
ADD ./cluster.yml /service

WORKDIR /service
