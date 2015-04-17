# Testing setup

To run the Scredis tests, we need

* Redis 3.0.0+
* Ruby for redis-trib
* `gem install redis`
* redis-server, redis-trib.rb on path


From `scredis` directory:

    ./start-redis-test-instances.sh
    
        
To shut it down:

    killall redis-server

