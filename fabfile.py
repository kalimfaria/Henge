#!/usr/bin/env python
from fabric.api import env, run
env.user = <your user name>
env.password = <your password>
env.hosts = [ 'node1', 'node2', 'node3', 'node4', 'node5' ]
def delete():
    run("cd /var/latencies && sudo rm -r *")

