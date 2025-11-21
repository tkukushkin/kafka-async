import confluent_kafka
import confluent_kafka.admin

from kafka_async import Consumer, Producer


def test_assign(consumer: Consumer, topic: str) -> None:
    consumer.assign([confluent_kafka.TopicPartition(topic, 0)])
    assert consumer.assignment() == [confluent_kafka.TopicPartition(topic, 0)]


def test_commit__no_args(consumer: Consumer, topic: str) -> None:
    consumer.commit()


async def test_commit__sync(producer: Producer, consumer: Consumer, topic: str) -> None:
    producer.produce(topic=topic)
    await producer.flush(timeout=5)

    consumer.subscribe([topic])
    assert await consumer.consume(timeout=5)

    result = await consumer.commit(asynchronous=False)

    assert result == [confluent_kafka.TopicPartition(topic, 0, 1)]

    assert await consumer.committed([confluent_kafka.TopicPartition(topic, 0)]) == [
        confluent_kafka.TopicPartition(topic, 0, 1)
    ]


async def test_commit__by_message_sync(producer: Producer, consumer: Consumer, topic: str) -> None:
    producer.produce(topic=topic)
    await producer.flush(timeout=5)

    consumer.subscribe([topic])
    (msg,) = await consumer.consume(timeout=5)

    result = await consumer.commit(msg, asynchronous=False)

    assert result == [confluent_kafka.TopicPartition(topic, 0, 1)]


async def test_commit__by_offsets_sync(producer: Producer, consumer: Consumer, topic: str) -> None:
    producer.produce(topic=topic)
    await producer.flush(timeout=5)

    consumer.subscribe([topic])
    assert await consumer.consume(timeout=5)

    result = await consumer.commit(offsets=[confluent_kafka.TopicPartition(topic, 0, 1)], asynchronous=False)

    assert result == [confluent_kafka.TopicPartition(topic, 0, 1)]


async def test_get_watermark_offsets__cached(producer: Producer, consumer: Consumer, topic: str) -> None:
    producer.produce(topic=topic)
    await producer.flush(timeout=5)

    assert consumer.get_watermark_offsets(confluent_kafka.TopicPartition(topic, 0), cached=True) == (
        confluent_kafka.OFFSET_INVALID,
        confluent_kafka.OFFSET_INVALID,
    )


async def test_get_watermark_offsets__not_cached(producer: Producer, consumer: Consumer, topic: str) -> None:
    producer.produce(topic=topic)
    await producer.flush(timeout=5)

    assert await consumer.get_watermark_offsets(confluent_kafka.TopicPartition(topic, 0), cached=False) == (0, 1)


def test_incremental_assign(consumer: Consumer) -> None:
    consumer.incremental_assign([confluent_kafka.TopicPartition('topic', 0, 0)])
    consumer.incremental_unassign([confluent_kafka.TopicPartition('topic', 0, 0)])


async def test_list_topics(consumer: Consumer) -> None:
    metadata = await consumer.list_topics()
    assert isinstance(metadata, confluent_kafka.admin.ClusterMetadata)


async def test_memberid(consumer: Consumer, topic: str, producer: Producer) -> None:
    producer.produce(topic=topic)
    await producer.flush(timeout=5)

    consumer.subscribe([topic])
    await consumer.consume()
    memberid = consumer.memberid()
    assert memberid
    assert memberid.startswith('rdkafka')


def test_consumer_group_metadata(consumer: Consumer) -> None:
    assert consumer.consumer_group_metadata() is not None


def test_set_sasl_credentials(consumer: Consumer) -> None:
    consumer.set_sasl_credentials('user', 'password')
