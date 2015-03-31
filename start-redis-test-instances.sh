#!/usr/bin/env bash

# the default redis server without config
redis-server --port 6379 &

# another redis server with config and auth
redis-server src/test/resources/redis-6380.conf &
