from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.producer import CryptoProducer
import threading
import logging
import sys
from typing import Dict

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  stream=sys.stdout
)

app = FastAPI()

threads: Dict[str, threading.Thread] = {}
producers: Dict[str, CryptoProducer] = {}
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

  if kafka_topic not in producers:
    producer = CryptoProducer(
      kafka_server=params.kafka_server,
      kafka_topic=kafka_topic,
      start_time=params.start_time,
      symbol=params.symbol,
      mode=params.mode
    )
    producers[kafka_topic] = producer

    with registry_lock:
      if kafka_topic in threads:
        return {"message": f"Stream for topic '{kafka_topic}' is already running."}

      # Run the blocking producer in a separate thread
      producer_thread = threading.Thread(
        target=producer.run_producer, daemon=True
      )
      threads[kafka_topic] = producer_thread
      producer_thread.start()
      logging.info(f"Started thread for Kafka topic '{kafka_topic}'.")

    return {"message": f"Stream for topic '{kafka_topic}' started successfully."}

  else:
    return {"message": f"Stream for topic '{kafka_topic}' is already running."}


@app.post("/stop-stream")
async def stop_stream(params: TopicRequest):
    kafka_topic = params.kafka_topic

    if not kafka_topic:
      raise HTTPException(status_code=400, detail="Missing 'kafka_topic'.")

    with registry_lock:
      producer = producers.pop(kafka_topic, None)
      producer_thread = threads.pop(kafka_topic, None)

    if not producer or not producer_thread:
      return {"message": f"No active stream found for topic '{kafka_topic}'."}

    try:
      # Gracefully stop the producer and join the thread
      producer.stop()
      producer_thread.join(timeout=5)
      logging.info(f"Stopped thread for Kafka topic '{kafka_topic}'.")
      return {"message": f"Stream for topic '{kafka_topic}' stopped successfully."}
    except Exception as e:
      raise HTTPException(status_code=500, detail=f"Failed to stop stream: {str(e)}")


@app.get("/stream-status")
async def stream_status():
  with registry_lock:
    statuses = {topic: producer.status() for topic, producer in producers.items()}
    return {"status": statuses}
