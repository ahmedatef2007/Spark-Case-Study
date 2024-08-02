from confluent_kafka import Producer
import json
import time
import random


conf = {
    'bootstrap.servers': 'pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'JUKQQM4ZM632RECA',
    'sasl.password': 'UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D',
}

# add topic_name here 
topic = 'omar_topic'

def generate_event(event_type):
    event = {
        "eventType": event_type,
        "customerId": str(random.randint(10000, 99999)),
        "productId": str(random.randint(1000, 9999)),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S'),
        "metadata": {}
    }
    
    if event_type == "productView":
        event["metadata"]["category"] = random.choice(["Electronics", "Clothing", "Home & Kitchen", "Books"])
        event["metadata"]["source"] = random.choice(["Advertisement", "Search", "Direct"])
    elif event_type == "addToCart":
        event["quantity"] = random.randint(1, 5)
    elif event_type == "purchase":
        event["quantity"] = random.randint(1, 5)
        event["totalAmount"] = round(random.uniform(10, 500), 2)
        event["paymentMethod"] = random.choice(["Credit Card", "Debit Card", "PayPal"])
    elif event_type == "recommendationClick":
        event["recommendedProductId"] = str(random.randint(1000, 9999))
        event["algorithm"] = random.choice(["collaborative_filtering", "content_based"])
    return event

producer = Producer(conf)

def send_event(event):
    try:
        producer.produce(topic, json.dumps(event))
        print("Event produced:", event)
    except Exception as e:
        print("Error:", e)

def main():
    while True:
        event_type = random.choice(["productView", "addToCart", "purchase", "recommendationClick"])
        event = generate_event(event_type)
        print(event)
        send_event(event)
        time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    main()
