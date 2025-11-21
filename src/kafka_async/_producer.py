import sys
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from datetime import datetime
from types import TracebackType
from typing import Any

import anyio
import anyio.to_thread
import confluent_kafka
import confluent_kafka.admin

from kafka_async._utils import async_to_sync, make_kwargs, make_sync_config, to_list

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Producer:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self.sync: confluent_kafka.Producer = confluent_kafka.Producer(
            make_sync_config(config),  # pyright: ignore[reportArgumentType]
        )

    def __len__(self) -> int:
        return len(self.sync)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        with anyio.CancelScope(shield=True):
            await self.flush()

    async def abort_transaction(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(lambda: self.sync.abort_transaction(**make_kwargs(timeout=timeout)))

    def begin_transaction(self) -> None:
        self.sync.begin_transaction()

    async def commit_transaction(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(lambda: self.sync.commit_transaction(**make_kwargs(timeout=timeout)))

    async def flush(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(lambda: self.sync.flush(**make_kwargs(timeout=timeout)))

    async def init_transactions(self, timeout: float | None = None) -> None:
        await anyio.to_thread.run_sync(lambda: self.sync.init_transactions(**make_kwargs(timeout=timeout)))

    async def list_topics(
        self, topic: str | None = None, timeout: float | None = None
    ) -> confluent_kafka.admin.ClusterMetadata:
        return await anyio.to_thread.run_sync(lambda: self.sync.list_topics(topic, **make_kwargs(timeout=timeout)))

    async def poll(self, timeout: float | None = None) -> int:
        return await anyio.to_thread.run_sync(lambda: self.sync.poll(**make_kwargs(timeout=timeout)))

    def produce(
        self,
        topic: str,
        value: str | bytes | None = None,
        *,
        key: str | bytes | None = None,
        partition: int | None = None,
        on_delivery: (
            Callable[[confluent_kafka.KafkaError | None, confluent_kafka.Message], Awaitable[None] | None] | None
        ) = None,
        timestamp: int | datetime | None = None,
        headers: Mapping[str, str | bytes | None] | Sequence[tuple[str, str | bytes | None]] | None = None,
    ) -> None:
        self.sync.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=async_to_sync(on_delivery) if on_delivery else None,
            headers=(
                [(k, v) for k, v in headers.items()]
                if isinstance(headers, Mapping)
                else to_list(headers)
                if isinstance(headers, Sequence)
                else None
            ),
            **make_kwargs(
                timestamp=int(timestamp.timestamp() * 1000) if isinstance(timestamp, datetime) else timestamp,
                partition=partition,
            ),
        )

    async def purge(self, in_queue: bool = True, in_flight: bool = True, blocking: bool = True) -> None:
        await anyio.to_thread.run_sync(self.sync.purge, in_queue, in_flight, blocking)

    async def send_offsets_to_transaction(
        self, positions: Iterable[confluent_kafka.TopicPartition], group_metadata: object, timeout: float | None = None
    ) -> None:
        await anyio.to_thread.run_sync(
            lambda: self.sync.send_offsets_to_transaction(
                to_list(positions), group_metadata, **make_kwargs(timeout=timeout)
            ),
        )

    def set_sasl_credentials(self, username: str, password: str) -> None:
        self.sync.set_sasl_credentials(username, password)
