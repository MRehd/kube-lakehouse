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
    mode:         str = 'async',   # kept for API parity; only 'async' is valid here
    max_intervals_per_pass: int = 12,   # cap intervals fetched concurrently per loop
    max_concurrent_requests: int = 4,   # bound aiohttp concurrency to avoid 429s + connector saturation
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
    # `aio_producer`, `http_session`, and `_sem` are async-loop-bound objects:
    # they must be created INSIDE an `async def` because they register
    # themselves with whatever event loop is currently running. We create
    # them lazily in `_async_feed_stream` for that reason; constructing them
    # in __init__ (which is sync) would tie them to whatever loop happens to
    # exist when the object is built, which may not be the loop that runs
    # the stream.
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
    # `async with self._sem` claims one slot in the per-stream semaphore.
    # If `max_concurrent_requests` requests are already in flight for this
    # stream, this line suspends the coroutine (other coroutines keep
    # running) until a slot frees up. The semaphore is released when this
    # `async with` block exits, even on exception.
    #
    # Chaining a second context manager (`self.http_session.get(url) as resp`)
    # in the same statement keeps cleanup orderly: aiohttp's underlying TCP
    # connection is returned to the pool when the response object is closed,
    # then the semaphore slot is released.
    #
    # `await resp.json()` reads the body off the socket without blocking the
    # event loop — while bytes are in flight, other coroutines can run.
    async with self._sem, self.http_session.get(url) as resp:
      resp.raise_for_status()
      return await resp.json()

  # ── Helpers (pure-Python, non-blocking) ─────────────────────────────────
  # These two methods are plain `def` (not `async def`) — they don't do I/O,
  # they're just CPU work over small lists. Calling them from a coroutine is
  # fine because they finish in microseconds; they don't need to suspend.
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
    # Lazy creation of the aiokafka producer: we need to be inside the running
    # event loop when we instantiate it (see __init__ note). The
    # `loop=asyncio.get_event_loop()` argument binds it to the current loop.
    if not self.aio_producer:
      self.aio_producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers = self.kafka_server,
        value_serializer  = lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer    = lambda k: k.encode('utf-8'),
        loop              = asyncio.get_event_loop(),
      )

    self.is_running = True
    last            = None

    # `aiohttp.TCPConnector(limit=20)` is the PROCESS-WIDE cap on simultaneous
    # TCP connections opened from this session. With multiple streams sharing
    # one process (FastAPI runs them all in the same loop), this prevents one
    # stream from monopolizing all sockets.
    #
    # `asyncio.Semaphore(max_concurrent_requests)` is the PER-STREAM cap. It
    # caps how many of those 20 sockets a single stream can hold at once,
    # leaving headroom for other streams' requests.
    #
    # Both `aiohttp.ClientSession(...)` and `self.aio_producer` are async
    # context managers. The combined `async with A, B:` enters A, then B, and
    # at scope exit closes B then A — so when the `while` loop ends (or an
    # exception propagates), the kafka producer flushes/closes and the HTTP
    # session is shut down cleanly. No leaked sockets, no half-open kafka
    # connections.
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session, self.aio_producer:
      self.http_session = session
      self._sem         = asyncio.Semaphore(self.max_concurrent_requests)

      while self.is_running:
        # On the first pass after a long-range start_time (historical catch-up
        # going back years), `_break_time_range` can produce 100k+ windows.
        # If we awaited gather() over ALL of them at once, we'd schedule
        # 100k coroutines simultaneously — each holding a semaphore slot,
        # each waiting on Coinbase — the event loop would choke and the
        # other stream's coroutines wouldn't get a turn for a long time.
        # Slicing to `max_intervals_per_pass` keeps each outer iteration short.
        intervals = self._break_time_range(self.start_time, self.end_time)
        intervals = intervals[: self.max_intervals_per_pass]

        # `asyncio.gather(*coroutines)` runs all the given coroutines
        # concurrently on the same event loop. The single `await` here
        # suspends this method until every coroutine finishes; while we
        # wait, OTHER coroutines (e.g. the BTC stream's loop, FastAPI
        # request handlers) keep running. The semaphore inside _get_data
        # ensures we don't actually have more than `max_concurrent_requests`
        # HTTP calls in flight at once even though we "scheduled" all of
        # them here.
        results = await asyncio.gather(*(self._get_data(s, e) for s, e in intervals))

        # Coinbase returns rows newest-first per window. Flatten all windows
        # then sort ascending so we can emit in chronological order and
        # compute `last` trivially as the max.
        rows = sorted(
          (row for data in results for row in data),
          key=lambda r: r[0],
        )

        # Producing to Kafka: aio_producer.send(...) returns a Future
        # immediately — it doesn't wait for the broker to acknowledge. We
        # collect the futures, then await them all together with gather()
        # below. This is much faster than awaiting each send individually
        # (which would serialize round-trips to the broker).
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

        # Wait for all the in-flight Kafka sends to be acknowledged, then
        # force any remaining buffered records out. flush() is `await`able
        # for the same reason: it does network I/O and must yield the loop
        # while waiting on the broker.
        if futures:
          await asyncio.gather(*futures)
          await self.aio_producer.flush()

        # Only back off when we've fully caught up to wall-clock — during
        # historical catch-up there's still data waiting, so loop right back
        # and process the next batch of intervals.
        self.end_time = datetime.utcnow().strftime(self.format_str)
        if last:
          time_diff_sec = (datetime.utcnow() - datetime.strptime(last, self.format_str)).total_seconds()
          if time_diff_sec < self.buffer:
            # We've caught up to within `buffer` seconds of now. Sleep
            # asynchronously: `await asyncio.sleep(s)` suspends THIS
            # coroutine for s seconds without blocking the loop, unlike
            # `time.sleep(s)` which would freeze every coroutine in the
            # process.
            await asyncio.sleep(max(self.buffer - time_diff_sec, 1))
          else:
            # `await asyncio.sleep(0)` is a cooperative yield: it doesn't
            # actually wait, but it returns control to the event loop so
            # other ready coroutines (e.g. the OTHER stream's iteration,
            # or a pending FastAPI request) get a chance to run. Without
            # this, a CPU-bound iteration that completes very quickly
            # could starve everyone else by never yielding back to the
            # loop. With it, BTC and ETH catch-up loops alternate fairly.
            await asyncio.sleep(0)

  # ── Control ─────────────────────────────────────────────────────────────
  # `stop` is a regular `def` because all it does is flip a flag. The next
  # time `_async_feed_stream` checks `while self.is_running:`, the loop
  # exits, the `async with` context managers close the HTTP session and
  # Kafka producer cleanly, and the coroutine returns.
  def stop(self) -> None:
    self.is_running = False

  def status(self) -> bool:
    return self.is_running
