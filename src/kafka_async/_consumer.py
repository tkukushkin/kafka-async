import sys
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping
from types import TracebackType
from typing import Any, Literal, overload

import anyio
import anyio.to_thread
import confluent_kafka
import confluent_kafka.admin

from ._utils import async_to_sync, make_kwargs, make_sync_config, to_list

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Consumer:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self.sync: confluent_kafka.Consumer = confluent_kafka.Consumer(
            make_sync_config(config),  # pyright: ignore[reportArgumentType]
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        with anyio.CancelScope(shield=True):
            await self.close()

    def assign(self, partitions: Iterable[confluent_kafka.TopicPartition]) -> None:
        self.sync.assign(to_list(partitions))

    def assignment(self) -> list[confluent_kafka.TopicPartition]:
        return self.sync.assignment()

    async def close(self) -> None:
        await anyio.to_thread.run_sync(self.sync.close)

    @overload
    def commit(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Iterable[confluent_kafka.TopicPartition] | None = None,
        *,
        asynchronous: Literal[True] = True,
    ) -> None: ...

    @overload
    async def commit(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Iterable[confluent_kafka.TopicPartition] | None = None,
        *,
        asynchronous: Literal[False],
    ) -> list[confluent_kafka.TopicPartition]: ...

    def commit(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Iterable[confluent_kafka.TopicPartition] | None = None,
        *,
        asynchronous: bool = True,
    ) -> None | Coroutine[Any, Any, list[confluent_kafka.TopicPartition]]:
        offsets_list = to_list(offsets) if offsets is not None else None
        kwargs = make_kwargs(message=message, offsets=offsets_list)
        if asynchronous:
            return self.sync.commit(**kwargs)  # pyright: ignore[reportUnknownVariableType]

        return anyio.to_thread.run_sync(lambda: self.sync.commit(asynchronous=False, **kwargs))  # pyright: ignore[reportUnknownVariableType,reportUnknownLambdaType]

    async def committed(
        self, partitions: Iterable[confluent_kafka.TopicPartition], timeout: float | None = None
    ) -> list[confluent_kafka.TopicPartition]:
        return await anyio.to_thread.run_sync(
            lambda: self.sync.committed(to_list(partitions), **make_kwargs(timeout=timeout))
        )

    async def consume(self, num_messages: int = 1, timeout: float | None = None) -> list[confluent_kafka.Message]:
        return await anyio.to_thread.run_sync(lambda: self.sync.consume(num_messages, **make_kwargs(timeout=timeout)))

    def consumer_group_metadata(self) -> object:
        return self.sync.consumer_group_metadata()

    @overload
    def get_watermark_offsets(
        self, partition: confluent_kafka.TopicPartition, *, cached: Literal[True]
    ) -> tuple[int, int]: ...

    @overload
    async def get_watermark_offsets(
        self, partition: confluent_kafka.TopicPartition, timeout: float | None = None, *, cached: Literal[False] = False
    ) -> tuple[int, int]: ...

    def get_watermark_offsets(
        self,
        partition: confluent_kafka.TopicPartition,
        timeout: float | None = None,
        cached: bool = False,
    ) -> tuple[int, int] | Coroutine[Any, Any, tuple[int, int]]:
        kwargs = make_kwargs(timeout=timeout)
        if cached:
            return self.sync.get_watermark_offsets(partition, cached=True, **kwargs)
        return anyio.to_thread.run_sync(lambda: self.sync.get_watermark_offsets(partition, cached=False, **kwargs))

    def incremental_assign(self, partitions: Iterable[confluent_kafka.TopicPartition]) -> None:
        self.sync.incremental_assign(to_list(partitions))

    def incremental_unassign(self, partitions: Iterable[confluent_kafka.TopicPartition]) -> None:
        self.sync.incremental_unassign(to_list(partitions))

    async def list_topics(
        self, topic: str | None = None, timeout: float | None = None
    ) -> confluent_kafka.admin.ClusterMetadata:
        return await anyio.to_thread.run_sync(lambda: self.sync.list_topics(topic, **make_kwargs(timeout=timeout)))

    def memberid(self) -> str | None:
        return self.sync.memberid()

    async def offsets_for_times(
        self, partitions: Iterable[confluent_kafka.TopicPartition], timeout: float | None = None
    ) -> list[confluent_kafka.TopicPartition]:
        return await anyio.to_thread.run_sync(
            lambda: self.sync.offsets_for_times(to_list(partitions), **make_kwargs(timeout=timeout))
        )

    def pause(self, partitions: Iterable[confluent_kafka.TopicPartition]) -> None:
        self.sync.pause(to_list(partitions))

    async def poll(self, timeout: float | None = None) -> confluent_kafka.Message | None:
        return await anyio.to_thread.run_sync(lambda: self.sync.poll(**make_kwargs(timeout=timeout)))

    def position(self, partitions: Iterable[confluent_kafka.TopicPartition]) -> list[confluent_kafka.TopicPartition]:
        return self.sync.position(to_list(partitions))

    def resume(self, partitions: Iterable[confluent_kafka.TopicPartition]) -> None:
        self.sync.resume(to_list(partitions))

    def seek(self, partition: confluent_kafka.TopicPartition) -> None:
        self.sync.seek(partition)

    def set_sasl_credentials(self, username: str, password: str) -> None:
        self.sync.set_sasl_credentials(username, password)

    def store_offsets(
        self,
        message: confluent_kafka.Message | None = None,
        offsets: Iterable[confluent_kafka.TopicPartition] | None = None,
    ) -> None:
        kwargs = make_kwargs(
            message=message,
            offsets=to_list(offsets) if offsets is not None else None,
        )
        self.sync.store_offsets(**kwargs)

    def subscribe(
        self,
        topics: Iterable[str],
        *,
        on_assign: (
            Callable[[confluent_kafka.Consumer, list[confluent_kafka.TopicPartition]], Awaitable[None] | None] | None
        ) = None,
        on_revoke: (
            Callable[[confluent_kafka.Consumer, list[confluent_kafka.TopicPartition]], Awaitable[None] | None] | None
        ) = None,
        on_lost: (
            Callable[[confluent_kafka.Consumer, list[confluent_kafka.TopicPartition]], Awaitable[None] | None] | None
        ) = None,
    ) -> None:
        self.sync.subscribe(
            to_list(topics),
            **make_kwargs(
                on_assign=async_to_sync(on_assign) if on_assign else None,
                on_revoke=async_to_sync(on_revoke) if on_revoke else None,
                on_lost=async_to_sync(on_lost) if on_lost else None,
            ),
        )

    def unassign(self) -> None:
        self.sync.unassign()

    def unsubscribe(self) -> None:
        self.sync.unsubscribe()
