#!/usr/bin/env bash

# the default redis server without config
redis-server --port 6379 &

# another redis server with config and auth
redis-server src/test/resources/redis-6380.conf &

# the redis cluster instances have their own respective directory
# clean the directory before starting, so we get a fresh cluster
find src/test/resources/cluster -mindepth 1 -maxdepth 1 -type d -execdir sh -c 'cd {}; rm appendonly.aof dump.rdp nodes.conf; redis-server ./redis.conf &' \;
redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005
