'''
Fully async variant of CryptoProducer — uses aiohttp for outgoing HTTP calls to
Coinbase instead of the sync `requests` library, and drops the sync
`_feed_stream` / `start()` / `run_producer()` branches entirely.

This is a drop-in replacement for utils.producer.CryptoProducer when running
under FastAPI (mode='async'): no thread hopping for I/O, a shared
aiohttp.ClientSession for connection pooling, and cleanup via async context
managers.

To use:
    # fast_app_asyncio.py
    from utils.producer_async import CryptoProducer

And add `aiohttp` to requirements.txt.
'''

import asyncio
import json
import logging
from datetime import datetime, timedelta
import aiohttp
import aiokafka


class CryptoProducer:

  format_str = '%Y-%m-%d %H:%M:%S'
  schema     = ['Timestamp', 'Low', 'High', 'Open', 'Close', 'Volume']

  def __init__(
    self,
    kafka_topic:  str = None,
    kafka_server: str = None,
    start_time:   str = None,
    end_time:     str = None,
    granularity:  int = 60,
    symbol:       str = None,
    window:       int = 5,
    buffer:       int = 60,
    mode:         str = 'async',   # kept for API parity; only 'async' is valid here
  ):
    self.kafka_topic  = kafka_topic
    self.kafka_server = kafka_server
    self.granularity  = granularity
    self.symbol       = symbol
    self.window       = window
    self.start_time   = start_time
    self.end_time     = end_time or datetime.utcnow().strftime(self.format_str)
    self.buffer       = buffer
    self.mode         = mode

    self.is_running   = False
    self.aio_producer = None
    self.http_session: aiohttp.ClientSession | None = None

  # ── HTTP ────────────────────────────────────────────────────────────────
  async def _get_data(self, start_time: str, end_time: str) -> list:
    '''Fetch OHLCV candles from Coinbase for a single [start, end] window.'''
    url = (
      f'https://api.exchange.coinbase.com/products/{self.symbol}/candles'
      f'?granularity={self.granularity}&start={start_time}&end={end_time}'
    )
    async with self.http_session.get(url) as resp:
      resp.raise_for_status()
      return await resp.json()

  # ── Helpers (pure-Python, non-blocking) ─────────────────────────────────
  def _break_time_range(self, start_time: str, end_time: str) -> list:
    start = datetime.strptime(start_time, self.format_str)
    end   = datetime.strptime(end_time,   self.format_str)
    intervals = []
    while start < end:
      current_end = min(start + timedelta(hours=self.window), end)
      intervals.append((start.strftime(self.format_str), current_end.strftime(self.format_str)))
      start = current_end
    return intervals

  def _transform_data(self, data) -> dict:
    return {
      col: datetime.utcfromtimestamp(val) if col == 'Timestamp' else float(val)
      for col, val in zip(self.schema, data)
    }

  # ── Main loop ───────────────────────────────────────────────────────────
  async def _async_feed_stream(self) -> None:
    if not self.aio_producer:
      self.aio_producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers = self.kafka_server,
        value_serializer  = lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer    = lambda k: k.encode('utf-8'),
        loop              = asyncio.get_event_loop(),
      )

    self.is_running = True
    last            = None

    # One HTTP session shared across all intervals — connection pooling keeps
    # repeated calls to the Coinbase API cheap.
    async with aiohttp.ClientSession() as session, self.aio_producer:
      self.http_session = session

      while self.is_running:
        intervals = self._break_time_range(self.start_time, self.end_time)

        # Fetch every interval concurrently — on a fresh run this is the
        # biggest win: the historical catch-up used to be serial.
        results = await asyncio.gather(*(self._get_data(s, e) for s, e in intervals))

        # Coinbase returns rows newest-first per window. Flatten all windows
        # then sort ascending so we can emit in chronological order and
        # compute `last` trivially as the max.
        rows = sorted(
          (row for data in results for row in data),
          key=lambda r: r[0],
        )

        futures    = []
        last_sent  = last  # dedup against the previous iteration's high-water mark
        for row in rows:
          event           = self._transform_data(row)
          event_timestamp = event['Timestamp'].strftime(self.format_str)
          if last_sent is None or event_timestamp > last_sent:
            futures.append(self.aio_producer.send(self.kafka_topic, key=event_timestamp, value=event))
            last_sent = event_timestamp

        last = last_sent
        self.start_time = last
        logging.info(self.start_time)

        if futures:
          await asyncio.gather(*futures)
          await self.aio_producer.flush()

        # Back off until the next minute of data is likely available.
        if last:
          time_diff_sec = (datetime.utcnow() - datetime.strptime(last, self.format_str)).total_seconds()
          if time_diff_sec < self.buffer:
            await asyncio.sleep(max(self.buffer - time_diff_sec, 1))

        self.end_time = datetime.utcnow().strftime(self.format_str)

  # ── Control ─────────────────────────────────────────────────────────────
  def stop(self) -> None:
    self.is_running = False

  def status(self) -> bool:
    return self.is_running
