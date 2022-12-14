#!/bin/sh


docker-compose down
docker-compose up -d
docker exec -it broker kafka-topics --bootstrap-server localhost:9092 --topic stock-transactions --partitions 5 --create