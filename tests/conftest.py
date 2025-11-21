import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid1

import confluent_kafka.admin
import pytest
from lovely.pytest.docker.compose import Services

from kafka_async import AdminClient, Consumer, Producer


@pytest.fixture(scope='session', autouse=True)
def anyio_backend() -> str:
    return 'asyncio'


@pytest.fixture(scope='session')
def kafka_addr() -> str:
    return '127.0.0.1:9092'


@pytest.fixture(scope='session')
def default_config(kafka_addr: str) -> dict[str, Any]:
    return {'bootstrap.servers': kafka_addr, 'topic.metadata.refresh.interval.ms': 1000}


@pytest.fixture(scope='session', autouse=True)
def start_kafka(default_config: dict[str, Any], docker_services: Services) -> None:
    docker_services.start('kafka')
    exception = None
    for _ in range(50):
        try:
            confluent_kafka.admin.AdminClient(default_config).list_topics(timeout=10)  # pyright: ignore[reportArgumentType]
        except confluent_kafka.KafkaException as exc:
            time.sleep(0.3)
            exception = exc
        else:
            return
    raise TimeoutError from exception


@pytest.fixture
async def producer(default_config: dict[str, Any]) -> AsyncIterator[Producer]:
    async with Producer(default_config) as producer:
        yield producer


@pytest.fixture
async def admin_client(default_config: dict[str, Any]) -> AdminClient:
    return AdminClient(default_config)


@pytest.fixture
async def consumer(default_config: dict[str, Any]) -> AsyncIterator[Consumer]:
    async with Consumer({**default_config, 'group.id': str(uuid1()), 'auto.offset.reset': 'earliest'}) as consumer:
        yield consumer


@pytest.fixture
def topic() -> str:
    return str(uuid1())
