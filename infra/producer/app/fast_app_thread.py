from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from infra.producer.app.utils.producer_thread import CryptoProducer
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
import sys
from typing import Dict

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  stream=sys.stdout
)

app = FastAPI()

executor = ThreadPoolExecutor()
streams: Dict[str, tuple] = {}
registry_lock = threading.Lock()

# ---- Request Schemas ---- #
class StreamRequest(BaseModel):
  kafka_server: str
  kafka_topic: str
  start_time: str
  symbol: str
  mode: str

class TopicRequest(BaseModel):
  kafka_topic: str

# ---- Async Endpoints ---- #

@app.post("/start-stream")
async def start_stream(params: StreamRequest):
  kafka_topic = params.kafka_topic

  if not kafka_topic:
    raise HTTPException(status_code=400, detail="Missing 'kafka_topic'.")

  with registry_lock:
    if kafka_topic in streams:
      return {"message": f"Stream for topic '{kafka_topic}' is already running."}

    producer = CryptoProducer(
      kafka_server=params.kafka_server,
      kafka_topic=kafka_topic,
      start_time=params.start_time,
      symbol=params.symbol,
      mode=params.mode
    )
    streams[kafka_topic] = (producer, executor.submit(producer.run_producer))
    logging.info(f"Started thread for Kafka topic '{kafka_topic}'.")

  return {"message": f"Stream for topic '{kafka_topic}' started successfully."}


@app.post("/stop-stream")
async def stop_stream(params: TopicRequest):
  kafka_topic = params.kafka_topic

  if not kafka_topic:
    raise HTTPException(status_code=400, detail="Missing 'kafka_topic'.")

  with registry_lock:
    stream = streams.pop(kafka_topic, None)

  if not stream:
    return {"message": f"No active stream found for topic '{kafka_topic}'."}

  producer, future = stream
  try:
    producer.stop()
    future.result(timeout=5)
    logging.info(f"Stopped thread for Kafka topic '{kafka_topic}'.")
    return {"message": f"Stream for topic '{kafka_topic}' stopped successfully."}
  except Exception as e:
    raise HTTPException(status_code=500, detail=f"Failed to stop stream: {str(e)}")


@app.get("/stream-status")
async def stream_status():
  with registry_lock:
    statuses = {topic: producer.status() for topic, (producer, _) in streams.items()}
    return {"status": statuses}
