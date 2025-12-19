from fastapi import FastAPI
from pydantic import BaseModel
import asyncio
from aiokafka import AIOKafkaProducer
import os
import json
from datetime import datetime

app = FastAPI(title="Orders Service")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "orders.events"
producer = None

class OrderRequest(BaseModel):
    userId: int
    itemId: int
    qty: int

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()

@app.post("/orders")
async def create_order(order: OrderRequest):
    order_created = {
        "type": "OrderCreated",
        "orderId": f"order-{int(datetime.now().timestamp()*1000)}",
        "userId": order.userId,
        "itemId": order.itemId,
        "qty": order.qty,
        "ts": datetime.utcnow().isoformat()
    }
    await producer.send_and_wait(TOPIC, json.dumps(order_created).encode("utf-8"))
    return {"status": "sent", "orderId": order_created["orderId"]}
