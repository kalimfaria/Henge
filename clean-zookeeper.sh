#!/usr/bin/env bash

sudo killall java
cd /var/zookeeper
sudo rm -r *
cd /tmp
sudo rm -r *.log
cd /var/nimbus/storm/logs
sudo rm -r *
