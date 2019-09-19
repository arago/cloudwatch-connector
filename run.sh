#!/bin/sh

mkdir -p /var/log/arago/cloudwatch

java -Xms1G -Xmx1G -cp /opt/arago/cloudwatch-connector/cloudwatch-connector-dist.jar -Djava.util.logging.config.file=/opt/arago/conf/cloudwatch-connector-logging.properties -Dlog4j.configuration=/opt/arago/conf/cloudwatch-connector-log4j.properties de.arago.connector.cloudwatch.CloudWatchMain