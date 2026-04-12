from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from utils.producer import CryptoProducer
import asyncio
import logging
import sys
from typing import Dict

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(message)s',
  stream=sys.stdout
)

app = FastAPI()

# No lock needed: asyncio is single-threaded and coroutines only yield at
# explicit `await` points, so dict access between awaits is inherently safe.
streams: Dict[str, tuple] = {}

# ---- Request Schemas ---- #
class StreamRequest(BaseModel):
  kafka_server: str
  kafka_topic: str
  start_time: str
  symbol: str

class TopicRequest(BaseModel):
  kafka_topic: str

# ---- Async Endpoints ---- #

@app.post("/start-stream")
async def start_stream(params: StreamRequest):
  kafka_topic = params.kafka_topic

  if not kafka_topic:
    raise HTTPException(status_code=400, detail="Missing 'kafka_topic'.")

  if kafka_topic in streams:
    return {"message": f"Stream for topic '{kafka_topic}' is already running."}

  producer = CryptoProducer(
    kafka_server=params.kafka_server,
    kafka_topic=kafka_topic,
    start_time=params.start_time,
    symbol=params.symbol,
    mode='async'
  )

  # create_task schedules the coroutine on the running event loop without
  # blocking this endpoint — equivalent to executor.submit() in the threaded version.
  # NOTE: _async_feed_stream uses time.sleep() which blocks the event loop during
  # the wait phase. Replace with `await asyncio.sleep()` in the producer for fully
  # non-blocking behaviour.
  task = asyncio.create_task(producer._async_feed_stream())
  streams[kafka_topic] = (producer, task)
  logging.info(f"Started task for Kafka topic '{kafka_topic}'.")

  return {"message": f"Stream for topic '{kafka_topic}' started successfully."}


@app.post("/stop-stream")
async def stop_stream(params: TopicRequest):
  kafka_topic = params.kafka_topic

  if not kafka_topic:
    raise HTTPException(status_code=400, detail="Missing 'kafka_topic'.")

  stream = streams.pop(kafka_topic, None)
  if not stream:
    return {"message": f"No active stream found for topic '{kafka_topic}'."}

  producer, task = stream
  try:
    producer.stop()  # sets is_running = False, exits the while loop gracefully
    task.cancel()    # interrupts any pending await (e.g. aiokafka flush)
    await task
  except asyncio.CancelledError:
    pass             # expected: task.cancel() raises CancelledError inside the coroutine
  except Exception as e:
    raise HTTPException(status_code=500, detail=f"Failed to stop stream: {str(e)}")

  logging.info(f"Stopped task for Kafka topic '{kafka_topic}'.")
  return {"message": f"Stream for topic '{kafka_topic}' stopped successfully."}


@app.get("/stream-status")
async def stream_status():
  return {"status": {topic: producer.status() for topic, (producer, _) in streams.items()}}
