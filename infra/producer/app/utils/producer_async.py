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

================================================================================
A 60-second tour of asyncio for readers new to it
================================================================================

Python's asyncio runs ONE thread that hosts an "event loop". The loop juggles
many coroutines (functions defined with `async def`) by running each one
until it hits a point where it has to wait for I/O — at that point the
coroutine "yields" control back to the loop, and the loop picks another
coroutine that's ready to run. When the I/O completes, the original coroutine
gets resumed.

Key vocabulary used in this file:

  async def       — declares a coroutine. Calling it returns a coroutine object;
                    it does not actually execute until awaited or scheduled.
  await X         — pauses the current coroutine until X (a coroutine, future,
                    or task) finishes; gives the loop a chance to run others.
                    Only legal inside an `async def`.
  asyncio.gather  — run several awaitables CONCURRENTLY and wait for all of
                    them. Concurrent ≠ parallel: one Python thread is still
                    running, but while one awaitable is blocked on a socket,
                    others can make progress.
  asyncio.create_task — schedule a coroutine to run in the background; returns
                    a Task you can await or cancel. Used in fast_app_asyncio.py
                    to spawn the per-topic producer loop.
  asyncio.Semaphore — a bounded counter. `async with sem:` waits until a slot
                    is free; used here to cap how many in-flight HTTP requests
                    a single stream can have.
  async with X    — like a regular `with`, but X's enter/exit are coroutines
                    that the loop can pause on (e.g. opening a TCP connection).

Critical rule: a synchronous blocking call (`time.sleep`, `requests.get`,
`socket.recv`) inside a coroutine STOPS THE WHOLE EVENT LOOP — every other
coroutine on the same loop is frozen until that call returns. That's exactly
the bug the original `requests`-based producer hit when two streams were
started: one stream's blocking HTTP call locked out the FastAPI handler
trying to start the second one. Everything in this file is either non-
blocking (aiohttp/aiokafka) or trivially fast (pure-Python list/dict ops).
================================================================================
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
    mode:         str = 'async',
    max_intervals_per_pass: int = 12,
    max_concurrent_requests: int = 4,
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
    self.max_intervals_per_pass  = max_intervals_per_pass
    self.max_concurrent_requests = max_concurrent_requests

    self.is_running   = False
    self.aio_producer = None
    self.http_session: aiohttp.ClientSession | None = None
    self._sem: asyncio.Semaphore | None = None

  # ── HTTP ────────────────────────────────────────────────────────────────
  async def _get_data(self, start_time: str, end_time: str) -> list:
    '''Fetch OHLCV candles from Coinbase for a single [start, end] window.'''
    url = (
      f'https://api.exchange.coinbase.com/products/{self.symbol}/candles'
      f'?granularity={self.granularity}&start={start_time}&end={end_time}'
    )
    async with self._sem, self.http_session.get(url) as resp:
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

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session, self.aio_producer:
      self.http_session = session
      self._sem         = asyncio.Semaphore(self.max_concurrent_requests)

      while self.is_running:
        intervals = self._break_time_range(self.start_time, self.end_time)
        intervals = intervals[: self.max_intervals_per_pass]

        results = await asyncio.gather(*(self._get_data(s, e) for s, e in intervals))

        rows = sorted(
          (row for data in results for row in data),
          key=lambda r: r[0],
        )

        futures    = []
        last_sent  = last
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

        self.end_time = datetime.utcnow().strftime(self.format_str)
        if not futures:
          await asyncio.sleep(self.buffer)
        elif last:
          time_diff_sec = (datetime.utcnow() - datetime.strptime(last, self.format_str)).total_seconds()
          if time_diff_sec < self.buffer:
            await asyncio.sleep(max(self.buffer - time_diff_sec, 1))
          else:
            await asyncio.sleep(0)

  # ── Control ─────────────────────────────────────────────────────────────
  def stop(self) -> None:
    self.is_running = False

  def status(self) -> bool:
    return self.is_running
