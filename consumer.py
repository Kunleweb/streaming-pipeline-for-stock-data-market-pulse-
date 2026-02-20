from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'stock_analysis',
    bootstrap_servers = ['localhost:9094'],
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    group_id = 'my-consumer-group',  #define consumer group
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)
print("Starting Kafka consumer. Waiting for messages on topic 'stock_analysis'.....")

for message in consumer:
    data = message.value
    #print the received data
    print(f"value(deserialized): {data}")
consumer.close()
print("kafka consumer closed")

