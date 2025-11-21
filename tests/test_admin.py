import sys
from collections.abc import Awaitable
from uuid import uuid1

import confluent_kafka.admin
import pytest

from kafka_async import AdminClient

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup


async def test_create_topics(admin_client: AdminClient) -> None:
    name = str(uuid1())
    result = await admin_client.create_topics([confluent_kafka.admin.NewTopic(name)])
    assert result == {name: None}


async def test_list_topics(admin_client: AdminClient) -> None:
    metadata = await admin_client.list_topics()
    assert isinstance(metadata, confluent_kafka.admin.ClusterMetadata)


async def test_describe_user_scram_credentials__no_users(admin_client: AdminClient) -> None:
    assert await admin_client.describe_user_scram_credentials() == {}


async def test_describe_user_scram_credentials__with_users_and_await_one(admin_client: AdminClient) -> None:
    result = admin_client.describe_user_scram_credentials(users=['user1'])
    assert list(result.keys()) == ['user1']
    assert isinstance(result['user1'], Awaitable)
    with pytest.raises(confluent_kafka.KafkaException):
        await result['user1']


async def test_describe_user_scram_credentials__with_users_and_await_all(admin_client: AdminClient) -> None:
    with pytest.raises(ExceptionGroup) as excinfo:
        await admin_client.describe_user_scram_credentials(users=['user1'])

    assert len(excinfo.value.exceptions) == 1
    assert isinstance(excinfo.value.exceptions[0], confluent_kafka.KafkaException)


def test_set_sasl_credentials(admin_client: AdminClient) -> None:
    admin_client.set_sasl_credentials('user', 'password')
