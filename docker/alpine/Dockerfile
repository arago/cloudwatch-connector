FROM openjdk:8-jre-alpine

# Set the locale(en_US.UTF-8)
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN apk update; apk upgrade;

COPY ./target/cloudwatch-connector-dist.jar /opt/arago/cloudwatch-connector/
COPY ./run.sh /opt/arago/cloudwatch-connector/
COPY ./conf/ /opt/arago/conf/

VOLUME /opt/arago/cloudwatch-connector/logs/

cmd /opt/arago/cloudwatch-connector/run.sh