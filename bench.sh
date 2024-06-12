#!/bin/bash

(
    make build-docker
    docker stop rotom
    docker run --rm -d --name rotom rotom
    sleep 3
    docker exec rotom redis-benchmark -t set,get,hset,rpush -P 10 > output_rotom
) &

wait

(
    docker stop redis
    docker run --rm -d --name redis redis
    sleep 3
    docker exec redis redis-benchmark -t set,get,hset,rpush -P 10 > output_redis
) &

wait

docker stop rotom
docker stop redis