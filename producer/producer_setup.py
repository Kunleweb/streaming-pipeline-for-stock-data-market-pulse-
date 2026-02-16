from kafka import KafkaProducer
import json 


topic = "stock_analysis"   #this is a table inside kafka for our record


def init_producer():
    producer = KafkaProducer(
        bootstrap_servers = 'localhost:9094',
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

    return producer