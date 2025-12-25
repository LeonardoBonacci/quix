# quix

```
docker exec -it broker kafka-topics --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it broker kafka-topics --list \
  --bootstrap-server localhost:9092  

docker exec -it broker kafka-console-producer \
  --topic test-topic \
  --bootstrap-server localhost:9092

docker exec -it broker kafka-console-consumer \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --from-beginning

docker run --rm --network host \
  confluentinc/cp-schema-registry:8.0.0 \
  kafka-avro-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic quix-avro-test \
    --from-beginning \
    --property schema.registry.url=http://localhost:8081


docker exec -it broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic transaction_requests2  

>{"transaction_id": "tx001", "amount": 120.0, "country": 0, "merchant": 0}
>{"transaction_id": "tx001", "amount": 120.0, "country": 0, "merchant": 0}
>{"transaction_id": "tx006", "amount": 75.0,  "country": 0, "merchant": 0}
>{"transaction_id": "tx007", "amount": 430.0, "country": 1, "merchant": 1}
>
```

