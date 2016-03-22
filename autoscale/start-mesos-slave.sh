#!/bin/bash

# If not used as post-install script, you can do the following before create image template
#	cp start-mesos-slave.sh /etc/init.d/
#	update-rc.d start-mesos-slave.sh defaults 

HOST_IP=$(ip addr show eth1 | awk '/inet / {print $2}' | cut -d/ -f1)
echo $HOST_IP > /etc/mesos-slave/ip
cp /etc/mesos-slave/ip /etc/mesos-slave/hostname

SLAVE_ATTRIBUTES='framework:acmeair'
echo $SLAVE_ATTRIBUTES >/etc/mesos-slave/attributes

rm -f /tmp/mesos/meta/slaves/latest

service mesos-slave restart