from quixstreams import Application
from quixstreams.models import SchemaRegistryClientConfig, SchemaRegistrySerializationConfig
from quixstreams.models.serializers.avro import AvroSerializer
from datetime import datetime
import time

# -------------------------------
# Schema Registry configuration
# -------------------------------
schema_registry_client_config = SchemaRegistryClientConfig(
    url="http://localhost:8081",
    basic_auth_user_info="username:password"  # only if needed
)

schema_registry_serialization_config = SchemaRegistrySerializationConfig(
    auto_register_schemas=True
)

# -------------------------------
# Avro schema
# -------------------------------
avro_schema = {
    "type": "record",
    "name": "MarketTick",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ],
}

# Build the AVRO serializer
serializer = AvroSerializer(
    schema=avro_schema,
    schema_registry_client_config=schema_registry_client_config,
    schema_registry_serialization_config=schema_registry_serialization_config
)

# -------------------------------
# Create Application
# -------------------------------
app = Application(broker_address="localhost:9092", consumer_group="quix-producer")

# -------------------------------
# Define topic with AVRO value_serializer
# -------------------------------
topic = app.topic(
    "quix-avro-test",
    value_serializer=serializer  # correct usage
)

# -------------------------------
# Produce messages
# -------------------------------
print("Producing messages…")
with app.get_producer() as producer:
    for i in range(10):
        msg = {
            "symbol2": "BTC-USD",
            "price2": 50000.0 + i * 10,
            "timestamp": int(datetime.utcnow().timestamp() * 1000)
        }
        # serialize with topic before sending
        serialized = topic.serialize(value=msg)
        producer.produce(
            topic=topic.name,
            key=serialized.key,
            value=serialized.value,
            timestamp=serialized.timestamp
        )
        print(f"Sent: {msg}")
        time.sleep(1)

print("Done sending")
