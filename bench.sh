#!/bin/bash

TEST_NAME=$1

IMAGE_NAME=$TEST_NAME

OUTPUT_FILE="output/$TEST_NAME"

COMMANDS="set,get,incr,lpush,rpush,hset,sadd,zadd"

PIPELINES=(1 10 100)

mkdir -p output

# clear output file
> $OUTPUT_FILE

docker run --rm -d --name bench-test $IMAGE_NAME
sleep 3

# run bench
for pipeline in "${PIPELINES[@]}"; do
    echo "Testing with pipeline: $pipeline" | tee -a $OUTPUT_FILE
    docker exec bench-test redis-benchmark --csv -t $COMMANDS -P $pipeline | tee -a $OUTPUT_FILE
    echo "" | tee -a $OUTPUT_FILE
done

docker stop bench-test

echo "Benchmarking completed. Results are saved in $OUTPUT_FILE."
