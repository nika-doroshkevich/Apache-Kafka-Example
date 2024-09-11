import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel, StrictStr


class ProducerResponse(BaseModel):
    name: StrictStr
    message_id: StrictStr
    topic: StrictStr
    timestamp: StrictStr = ""


class ProducerMessage(BaseModel):
    name: StrictStr
    message_id: StrictStr = ""
    timestamp: StrictStr = ""


app = FastAPI()

KAFKA_INSTANCE = "localhost:29092"

loop = asyncio.get_event_loop()

aio_producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_INSTANCE)

consumer = AIOKafkaConsumer("test1", bootstrap_servers=KAFKA_INSTANCE, loop=loop)


async def consume():
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "consumed: ",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.key,
                msg.value,
                msg.timestamp,
            )

    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup_event():
    await aio_producer.start()
    loop.create_task(consume())


@app.on_event("shutdown")
async def shutdown_event():
    await aio_producer.stop()
    await consumer.stop()


@app.get("/")
def read_root():
    return {"Ping": "Pong"}


@app.post("/producer/{topic_name}")
async def kafka_produce(msg: ProducerMessage, topic_name: str):
    await aio_producer.send(topic_name, json.dumps(msg.dict()).encode("ascii"))
    response = ProducerResponse(
        name=msg.name, message_id=msg.message_id, topic=topic_name
    )

    return response
