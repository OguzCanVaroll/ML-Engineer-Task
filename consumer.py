from kafka import KafkaConsumer
import json

products = ["aa"]

def consume_messages():
    consumer = KafkaConsumer(
        'my-topic',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        print(f"Received message: {message.value}")
        products.append(message.value)

if __name__ == "__main__":
    consume_messages()
