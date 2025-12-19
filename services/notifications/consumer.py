import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CONSUME_TOPIC = "orders.events"
PRODUCE_TOPIC = "orders.events"

async def consume():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="notifications-group"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            if event.get("type") == "PaymentCaptured":
                email_event = {
                    "type": "EmailSent",
                    "orderId": event["orderId"],
                    "to": f"user{event['orderId']}@example.com",
                    "ts": datetime.utcnow().isoformat()
                }
                await producer.send_and_wait(PRODUCE_TOPIC, json.dumps(email_event).encode("utf-8"))
                print(f"Email sent for order {event['orderId']}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
