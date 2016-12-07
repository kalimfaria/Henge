#!/usr/bin/env bash

sudo apt-get update
sudo apt-get install vim
sudo apt-get install curl
sudo apt-get install jq
sudo apt-get install tmux

cd /var/nimbus
sudo cp ~/libsigar-amd64-linux.so .
sudo cp ~/sysinfo-1.0-SNAPSHOT.one-jar.jar .
