from datetime import datetime, timezone
from uuid import uuid1

import confluent_kafka
from confluent_kafka import KafkaError, Message

from kafka_async import Consumer, Producer


async def test_produce(producer: Producer, consumer: Consumer):
    topic = str(uuid1())
    value = str(uuid1())
    date = datetime(2023, 1, 1, tzinfo=timezone.utc)

    on_delivery_err: confluent_kafka.KafkaError | None = None
    on_delivery_msg: confluent_kafka.Message | None = None

    async def on_delivery(err: KafkaError | None, msg: Message | None) -> None:
        nonlocal on_delivery_err, on_delivery_msg
        on_delivery_err = err
        on_delivery_msg = msg

    producer.produce(
        topic=topic,
        value=value,
        on_delivery=on_delivery,
        headers={'a': 'string', 'b': b'bytes', 'c': None},
        timestamp=date,
    )
    await producer.flush(timeout=10)

    assert on_delivery_err is None
    assert on_delivery_msg
    assert on_delivery_msg.topic() == topic
    assert on_delivery_msg.value() == value.encode('ascii')

    consumer.subscribe([topic])
    (msg,) = await consumer.consume(timeout=5)
    assert msg.headers() == [('a', b'string'), ('b', b'bytes'), ('c', None)]
    assert msg.timestamp() == (confluent_kafka.TIMESTAMP_CREATE_TIME, int(date.timestamp() * 1000))


async def test_purge(producer: Producer) -> None:
    topic = str(uuid1())

    async def on_delivery(err: KafkaError | None, msg: Message | None) -> None:
        assert err
        assert err.code() == confluent_kafka.KafkaError._PURGE_QUEUE

    producer.produce(topic=topic, on_delivery=on_delivery)
    await producer.purge()
    await producer.flush(timeout=10)


async def test_poll(producer: Producer) -> None:
    topic = str(uuid1())
    called = False

    async def on_delivery(err: KafkaError | None, msg: Message | None) -> None:
        nonlocal called
        called = True

    producer.produce(topic=topic, on_delivery=on_delivery)
    polled = await producer.poll(timeout=5)

    assert called
    assert polled == 1

    polled = await producer.poll(timeout=0.1)
    assert polled == 0


async def test__len__(producer: Producer) -> None:
    topic = str(uuid1())
    assert len(producer) == 0
    producer.produce(topic=topic)
    assert len(producer) == 1


async def test_list_topics__all(producer: Producer) -> None:
    topic = str(uuid1())
    producer.produce(topic=topic)
    await producer.flush(timeout=10)
    topics = await producer.list_topics()
    assert topic in topics.topics


async def test_list_topics__one(producer: Producer) -> None:
    topic = str(uuid1())
    producer.produce(topic=topic)
    await producer.flush(timeout=10)
    topics = await producer.list_topics(topic)
    assert topic in topics.topics


def test_set_sasl_credentials(producer: Producer) -> None:
    producer.set_sasl_credentials('user', 'password')
