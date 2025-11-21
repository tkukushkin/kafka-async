import logging
from collections.abc import Coroutine, Iterable, Mapping
from concurrent.futures import Future
from typing import Any, cast, overload

import anyio
import anyio.to_thread
import confluent_kafka
import confluent_kafka.admin

from ._utils import FuturesDict, make_kwargs, make_sync_config, to_dict, to_list, to_set, wrap_concurrent_future


class AdminClient:
    def __init__(self, config: Mapping[str, Any], *, logger: logging.Logger | None = None) -> None:
        self.sync: confluent_kafka.admin.AdminClient = confluent_kafka.admin.AdminClient(
            make_sync_config(config),  # pyright: ignore[reportArgumentType]
            logger=logger,
        )

    def create_topics(
        self,
        new_topics: Iterable[confluent_kafka.admin.NewTopic],
        *,
        operation_timeout: float | None = None,
        request_timeout: float | None = None,
        validate_only: bool = False,
    ) -> FuturesDict[str, None]:
        return FuturesDict[str, None].from_concurrent_futures(
            self.sync.create_topics(
                to_list(new_topics),
                validate_only=validate_only,
                **make_kwargs(operation_timeout=operation_timeout, request_timeout=request_timeout),
            )
        )

    def delete_topics(
        self,
        topics: Iterable[str],
        *,
        operation_timeout: float | None = None,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, None]:
        return FuturesDict[str, None].from_concurrent_futures(
            self.sync.delete_topics(
                to_list(topics), **make_kwargs(operation_timeout=operation_timeout, request_timeout=request_timeout)
            )
        )

    async def list_topics(
        self, topic: str | None = None, timeout: float | None = None
    ) -> confluent_kafka.admin.ClusterMetadata:
        return await anyio.to_thread.run_sync(lambda: self.sync.list_topics(topic, **make_kwargs(timeout=timeout)))

    def create_partitions(
        self,
        new_partitions: Iterable[confluent_kafka.admin.NewPartitions],
        *,
        operation_timeout: float | None = None,
        request_timeout: float | None = None,
        validate_only: bool = False,
    ) -> FuturesDict[str, None]:
        return FuturesDict[str, None].from_concurrent_futures(
            self.sync.create_partitions(
                to_list(new_partitions),
                validate_only=validate_only,
                **make_kwargs(operation_timeout=operation_timeout, request_timeout=request_timeout),
            )
        )

    def describe_configs(
        self,
        resources: Iterable[confluent_kafka.admin.ConfigResource],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.admin.ConfigResource, dict[str, confluent_kafka.admin.ConfigEntry]]:
        return FuturesDict[
            confluent_kafka.admin.ConfigResource, dict[str, confluent_kafka.admin.ConfigEntry]
        ].from_concurrent_futures(
            self.sync.describe_configs(to_list(resources), **make_kwargs(request_timeout=request_timeout))
        )

    def incremental_alter_configs(
        self,
        resources: Iterable[confluent_kafka.admin.ConfigResource],
        *,
        request_timeout: float | None = None,
        validate_only: bool = False,
        broker: int | None = None,
    ) -> FuturesDict[confluent_kafka.admin.ConfigResource, None]:
        return FuturesDict[confluent_kafka.admin.ConfigResource, None].from_concurrent_futures(
            self.sync.incremental_alter_configs(
                to_list(resources),
                validate_only=validate_only,
                **make_kwargs(request_timeout=request_timeout, broker=broker),
            )
        )

    def create_acls(
        self,
        acls: Iterable[confluent_kafka.admin.AclBinding],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.admin.AclBinding, None]:
        return FuturesDict[confluent_kafka.admin.AclBinding, None].from_concurrent_futures(
            self.sync.create_acls(to_list(acls), **make_kwargs(request_timeout=request_timeout))
        )

    async def describe_acls(
        self,
        acl_binding_filter: confluent_kafka.admin.AclBindingFilter,
        *,
        request_timeout: float | None = None,
    ) -> list[confluent_kafka.admin.AclBinding]:
        return await wrap_concurrent_future(
            self.sync.describe_acls(acl_binding_filter, **make_kwargs(request_timeout=request_timeout))
        )

    def delete_acls(
        self,
        acl_binding_filters: Iterable[confluent_kafka.admin.AclBindingFilter],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.admin.AclBindingFilter, list[confluent_kafka.admin.AclBinding]]:
        return FuturesDict[
            confluent_kafka.admin.AclBindingFilter, list[confluent_kafka.admin.AclBinding]
        ].from_concurrent_futures(
            self.sync.delete_acls(to_list(acl_binding_filters), **make_kwargs(request_timeout=request_timeout))
        )

    async def list_consumer_groups(
        self,
        *,
        request_timeout: float | None = None,
        states: Iterable[confluent_kafka.ConsumerGroupState] | None = None,
        types: Iterable[confluent_kafka.ConsumerGroupType] | None = None,
    ) -> confluent_kafka.admin.ListConsumerGroupsResult:
        return await wrap_concurrent_future(
            self.sync.list_consumer_groups(
                **make_kwargs(
                    request_timeout=request_timeout,
                    states=to_set(states) if states is not None else None,
                    types=to_set(types) if types is not None else None,
                )
            )
        )

    def describe_consumer_groups(
        self,
        group_ids: Iterable[str],
        *,
        include_authorized_operations: bool = False,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.admin.ConsumerGroupDescription]:
        return FuturesDict[str, confluent_kafka.admin.ConsumerGroupDescription].from_concurrent_futures(
            self.sync.describe_consumer_groups(
                to_list(group_ids),
                include_authorized_operations=include_authorized_operations,
                **make_kwargs(request_timeout=request_timeout),
            )
        )

    def describe_topics(
        self,
        topics: confluent_kafka.TopicCollection | Iterable[str],
        *,
        include_authorized_operations: bool = False,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.admin.TopicDescription]:
        return FuturesDict[str, confluent_kafka.admin.TopicDescription].from_concurrent_futures(
            self.sync.describe_topics(
                confluent_kafka.TopicCollection(to_list(topics)) if isinstance(topics, Iterable) else topics,
                include_authorized_operations=include_authorized_operations,
                **make_kwargs(request_timeout=request_timeout),
            )
        )

    async def describe_cluster(
        self,
        *,
        include_authorized_operations: bool = False,
        request_timeout: float | None = None,
    ) -> confluent_kafka.admin.DescribeClusterResult:
        return await wrap_concurrent_future(
            self.sync.describe_cluster(
                include_authorized_operations=include_authorized_operations,
                **make_kwargs(request_timeout=request_timeout),
            )
        )

    def delete_consumer_groups(
        self, group_ids: Iterable[str], *, request_timeout: float | None = None
    ) -> FuturesDict[str, None]:
        return FuturesDict[str, None].from_concurrent_futures(
            self.sync.delete_consumer_groups(to_list(group_ids), **make_kwargs(request_timeout=request_timeout))
        )

    def list_consumer_group_offsets(
        self,
        list_consumer_group_offsets_request: Iterable[confluent_kafka.ConsumerGroupTopicPartitions],
        *,
        require_stable: bool = False,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, confluent_kafka.ConsumerGroupTopicPartitions]:
        return FuturesDict[str, confluent_kafka.ConsumerGroupTopicPartitions].from_concurrent_futures(
            self.sync.list_consumer_group_offsets(
                to_list(list_consumer_group_offsets_request),
                require_stable=require_stable,
                **make_kwargs(request_timeout=request_timeout),
            )
        )

    def alter_consumer_group_offsets(
        self,
        alter_consumer_group_offsets_request: Iterable[confluent_kafka.ConsumerGroupTopicPartitions],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.ConsumerGroupTopicPartitions, confluent_kafka.ConsumerGroupTopicPartitions]:
        return FuturesDict[
            confluent_kafka.ConsumerGroupTopicPartitions, confluent_kafka.ConsumerGroupTopicPartitions
        ].from_concurrent_futures(
            self.sync.alter_consumer_group_offsets(
                to_list(alter_consumer_group_offsets_request), **make_kwargs(request_timeout=request_timeout)
            )
        )

    def set_sasl_credentials(self, username: str, password: str) -> None:
        self.sync.set_sasl_credentials(username, password)

    @overload
    async def describe_user_scram_credentials(
        self, users: None = None, *, request_timeout: float | None = None
    ) -> dict[str, confluent_kafka.admin.UserScramCredentialsDescription]: ...

    @overload
    def describe_user_scram_credentials(
        self, users: Iterable[str], *, request_timeout: float | None = None
    ) -> FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription]: ...

    @overload
    def describe_user_scram_credentials(
        self, users: None | Iterable[str], *, request_timeout: float | None = None
    ) -> (
        Coroutine[Any, Any, dict[str, confluent_kafka.admin.UserScramCredentialsDescription]]
        | FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription]
    ): ...

    def describe_user_scram_credentials(
        self,
        users: Iterable[str] | None = None,
        *,
        request_timeout: float | None = None,
    ) -> (
        Coroutine[Any, Any, dict[str, confluent_kafka.admin.UserScramCredentialsDescription]]
        | FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription]
    ):
        kwargs = make_kwargs(request_timeout=request_timeout)
        if users is None:
            future = cast(
                Future[dict[str, confluent_kafka.admin.UserScramCredentialsDescription]],
                self.sync.describe_user_scram_credentials(**kwargs),
            )
            return wrap_concurrent_future(future)
        futures_dict = self.sync.describe_user_scram_credentials(to_list(users), **kwargs)
        assert isinstance(futures_dict, dict)
        return FuturesDict[str, confluent_kafka.admin.UserScramCredentialsDescription].from_concurrent_futures(
            futures_dict
        )

    def alter_user_scram_credentials(
        self,
        alterations: Iterable[confluent_kafka.admin.UserScramCredentialAlteration],
        *,
        request_timeout: float | None = None,
    ) -> FuturesDict[str, None]:
        return FuturesDict[str, None].from_concurrent_futures(
            self.sync.alter_user_scram_credentials(to_list(alterations), **make_kwargs(request_timeout=request_timeout))
        )

    def list_offsets(
        self,
        topic_partition_offsets: Mapping[confluent_kafka.TopicPartition, confluent_kafka.admin.OffsetSpec],
        *,
        isolation_level: confluent_kafka.IsolationLevel | None = None,
        request_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.TopicPartition, confluent_kafka.admin.ListOffsetsResultInfo]:
        return FuturesDict[
            confluent_kafka.TopicPartition, confluent_kafka.admin.ListOffsetsResultInfo
        ].from_concurrent_futures(
            self.sync.list_offsets(
                to_dict(topic_partition_offsets),
                **make_kwargs(isolation_level=isolation_level, request_timeout=request_timeout),
            )
        )

    def delete_records(
        self,
        topic_partition_offsets: Iterable[confluent_kafka.TopicPartition],
        *,
        request_timeout: float | None = None,
        operation_timeout: float | None = None,
    ) -> FuturesDict[confluent_kafka.TopicPartition, confluent_kafka.admin.DeletedRecords]:
        return FuturesDict[
            confluent_kafka.TopicPartition, confluent_kafka.admin.DeletedRecords
        ].from_concurrent_futures(
            self.sync.delete_records(
                to_list(topic_partition_offsets),
                **make_kwargs(request_timeout=request_timeout, operation_timeout=operation_timeout),
            )
        )

    async def elect_leaders(
        self,
        election_type: confluent_kafka.ElectionType,
        partitions: Iterable[confluent_kafka.TopicPartition] | None = None,
        *,
        request_timeout: float | None = None,
        operation_timeout: float | None = None,
    ) -> dict[confluent_kafka.TopicPartition, confluent_kafka.KafkaException | None]:
        return await wrap_concurrent_future(
            self.sync.elect_leaders(
                election_type,
                to_list(partitions) if partitions is not None else None,
                **make_kwargs(request_timeout=request_timeout, operation_timeout=operation_timeout),
            )
        )
