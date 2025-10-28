#!/usr/bin/env bash
set -euo pipefail
CID=$(docker ps --filter "ancestor=confluentinc/cp-kafka:7.6.1" -q | head -n1)
docker exec -it "$CID" kafka-topics --create --topic raw_msgs --partitions 6 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092
docker exec -it "$CID" kafka-topics --create --topic deduped_msgs --partitions 6 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092