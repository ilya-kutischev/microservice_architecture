import asyncio
import functools
import logging
import signal
import sys
import confluent_kafka

from confluent_kafka import KafkaException
from time import time
from threading import Thread

"""============================CONSUMER LOGIC==================================="""
signal.signal(signal.SIGTERM, lambda *args: sys.exit())
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

config = {
    # "group.id": "consumer-group-name",
    "bootstrap.servers": "localhost:9092",
}


async def consume(config, topic):
    consumer = confluent_kafka.Consumer(config)
    consumer.subscribe([topic])
    loop = asyncio.get_running_loop()
    poll = functools.partial(consumer.poll, 0.1)
    try:
        log.info(f"Starting consumer: {topic}")
        while True:
            message = await loop.run_in_executor(None, poll)
            if message is None:
                continue
            if message.error():
                log.error("Consumer error: {}".format(message.error()))
                continue
            # TODO: Inject topic-specific logic here
            log.info(f"Consuming message: {message.value()}")
    finally:
        log.info(f"Closing consumer: {topic}")
        consumer.close()


consume = functools.partial(consume, config)


async def run_consumer():
    config = {
        "group.id": "consumer-group-name",
        "bootstrap.servers": "localhost:9092",
    }
    await asyncio.gather(
        # consume("topic1"),
        consume(config,"auth_search"),
        consume(config,"stats_search"),
    )

if __name__ == "__main__":
    try:
        # asyncio.run(main())
        loop = asyncio.get_running_loop()
        loop.create_task(run_consumer())

    except (KeyboardInterrupt, SystemExit):
        log.info("Application shutdown complete")

"""============================PRODUCER LOGIC==================================="""


class AIOProducer:
    def __init__(self, configs, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value):
        """
        An awaitable produce method.
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result

    def produce2(self, topic, value, on_delivery):
        """
        A produce method in which delivery notifications are made available
        via both the returned future and on_delivery callback (if specified).
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(
                    result.set_result, msg)
            if on_delivery:
                self._loop.call_soon_threadsafe(
                    on_delivery, err, msg)
        self._producer.produce(topic, value, on_delivery=ack)
        return result


class Producer:
    def __init__(self, configs):
        self._producer = confluent_kafka.Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, value, on_delivery=None):
        self._producer.produce(topic, value, on_delivery=on_delivery)


config = {"bootstrap.servers": "localhost:9092"}




