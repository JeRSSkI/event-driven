import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CONSUME_TOPIC = "orders.events"
PRODUCE_TOPIC = "orders.events"
processed_events = set()  # для ідемпотентності

async def consume():
    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="billing-group"
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            if event.get("type") == "OrderCreated":
                order_id = event["orderId"]
                if order_id in processed_events:
                    continue  # ідемпотентність
                processed_events.add(order_id)
                # симуляція платежу
                payment = {
                    "type": "PaymentCaptured",
                    "orderId": order_id,
                    "amount": event["qty"] * 10.0,
                    "ts": datetime.utcnow().isoformat()
                }
                await producer.send_and_wait(PRODUCE_TOPIC, json.dumps(payment).encode("utf-8"))
                print(f"Processed payment for order {order_id}")
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(consume())
