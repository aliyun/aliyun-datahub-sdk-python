.. _api:

*************
API Reference
*************


.. _datahub_client:

DataHub
=======

.. autoclass:: datahub.models.compress.CompressFormat
    :members:

.. autoclass:: datahub.DataHub
    :members:

Auth
====

.. autoclass:: datahub.auth.AccountType
    :members:

.. autoclass:: datahub.auth.Account
    :members:

.. autoclass:: datahub.auth.AliyunAccount
    :members:

.. _schema:

Schema
=========

.. autoclass:: datahub.models.FieldType
    :members:

.. autoclass:: datahub.models.Field
    :members:

.. autoclass:: datahub.models.RecordSchema
    :members:

.. _record:

Record
=======

.. autoclass:: datahub.models.RecordType
    :members:

.. autoclass:: datahub.models.BlobRecord
    :members:

.. autoclass:: datahub.models.TupleRecord
    :members:

.. autoclass:: datahub.models.FailedRecord
    :members:

.. _shard:

Shard
======

.. autoclass:: datahub.models.ShardState
    :members:

.. autoclass:: datahub.models.ShardBase
    :members:

.. autoclass:: datahub.models.Shard
    :members:

.. autoclass:: datahub.models.ShardContext
    :members:

Cursor
=======

.. autoclass:: datahub.models.CursorType
    :members:


.. _offset:

Offset
===========

.. autoclass:: datahub.models.OffsetBase
    :members:

.. autoclass:: datahub.models.OffsetWithVersion
    :members:

.. autoclass:: datahub.models.OffsetWithSession
    :members:

.. _subscription:

Subscription
============

.. autoclass:: datahub.models.SubscriptionState
    :members:

.. autoclass:: datahub.models.Subscription
    :members:

.. _connector:

Connector
============

.. autoclass:: datahub.models.ConnectorState
    :members:

.. autoclass:: datahub.models.AuthMode
    :members:

.. autoclass:: datahub.models.PartitionMode
    :members:

.. autoclass:: datahub.models.ConnectorType
    :members:

.. autoclass:: datahub.models.OdpsConnectorConfig
    :members:

.. autoclass:: datahub.models.DatabaseConnectorConfig
    :members:

.. autoclass:: datahub.models.EsConnectorConfig
    :members:

.. autoclass:: datahub.models.FcConnectorConfig
    :members:

.. autoclass:: datahub.models.OssConnectorConfig
    :members:

.. autoclass:: datahub.models.OtsConnectorConfig
    :members:

.. _results:

Results
=======

.. autoclass:: datahub.models.results.ListProjectResult
    :members:

.. autoclass:: datahub.models.results.GetProjectResult
    :members:

.. autoclass:: datahub.models.results.ListTopicResult
    :members:

.. autoclass:: datahub.models.results.GetTopicResult
    :members:

.. autoclass:: datahub.models.results.ListShardResult
    :members:

.. autoclass:: datahub.models.results.MergeShardResult
    :members:

.. autoclass:: datahub.models.results.SplitShardResult
    :members:

.. autoclass:: datahub.models.results.GetCursorResult
    :members:

.. autoclass:: datahub.models.results.PutRecordsResult
    :members:

.. autoclass:: datahub.models.results.GetRecordsResult
    :members:

.. autoclass:: datahub.models.results.GetMeteringInfoResult
    :members:

.. autoclass:: datahub.models.results.ListConnectorResult
    :members:

.. autoclass:: datahub.models.results.GetConnectorResult
    :members:

.. autoclass:: datahub.models.results.GetConnectorShardStatusResult
    :members:

.. autoclass:: datahub.models.results.InitAndGetSubscriptionOffsetResult
    :members:

.. autoclass:: datahub.models.results.GetSubscriptionOffsetResult
    :members:

.. autoclass:: datahub.models.results.CreateSubscriptionResult
    :members:

.. autoclass:: datahub.models.results.GetSubscriptionResult
    :members:

.. autoclass:: datahub.models.results.ListSubscriptionResult
    :members:

Exceptions
==========

.. autoclass:: datahub.exceptions.DatahubException
    :members:

.. autoclass:: datahub.exceptions.ResourceExistException
    :members:

.. autoclass:: datahub.exceptions.ResourceNotFoundException
    :members:

.. autoclass:: datahub.exceptions.InvalidParameterException
    :members:

.. autoclass:: datahub.exceptions.InvalidOperationException
    :members:

.. autoclass:: datahub.exceptions.LimitExceededException
    :members:

.. autoclass:: datahub.exceptions.InternalServerException
    :members:

.. autoclass:: datahub.exceptions.AuthorizationFailedException
    :members:

.. autoclass:: datahub.exceptions.NoPermissionException
    :members:
