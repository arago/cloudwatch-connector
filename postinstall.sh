#!/bin/sh

chmod +x /etc/init.d/cloudwatch-connector

SERVICE_NAME=cloudwatch-connector
/sbin/chkconfig --add $SERVICE_NAME
if [ -e /etc/SuSE-release ]; then
    /sbin/chkconfig $SERVICE_NAME 35
else
    /sbin/chkconfig --level 35 $SERVICE_NAME on
fi

mkdir -p /var/log/autopilot/cloudwatch
chown -R arago:arago /var/log/autopilot/cloudwatch

exit 0;
