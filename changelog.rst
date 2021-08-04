Changelog
================

Version 2.17.2
-----------------

+ support to create topic with extend mode

Version 2.17.1
-----------------

+ fix invalid shard status

Version 2.17.0
-----------------

+ fix field length longer than schema specified

Version 2.16.0
-----------------

+ fix python3 blob_data type error in pb mode
+ support new date type

Version 2.15.2
-----------------

+ fix bool cast

Version 2.15.1
-----------------

+ remove string length check

Version 2.15.0
-----------------

+ add update project api
+ validate param type
+ list_connector will return connector_ids
+ create_connector will return connector_id
+ param connector_type change to connector_id
+ deprecated shard_contexts in GetConnectorResult
+ deprecated ConnectorState `CONNECTOR_CREATED`, `CONNECTOR_PAUSED`, add `CONNECTOR_STOPPED`
+ member variable in GetConnectorShardStatusResult is changed, not compatible
+ remove max_commit_size in DatabaseConnectorConfig,EsConnectorConfig, not compatible
+ remove invocation_role,batch_size,start_position,start_timestamp in FcConnectorConfig, not compatible
+ add start_time in CreateConnectorParams

Version 2.12.7
-----------------

+ specify cprotobuf version to 0.1.9

Version 2.12.6
-----------------

+ add update connector offset

Version 2.12.5
-----------------

+ fix double field convert precision

Version 2.12.4
-----------------

+ fix user define mode connector config param

Version 2.12.3
-----------------

+ fix user define mode get connector key not found

Version 2.12.2
-----------------

+ fix None value for bool field in record

Version 2.12.1
-----------------

+ fix implement bug

Version 2.12.0
-----------------

+ update connector
+ put record by shard id

Version 2.11.5
-----------------

+ some compatibility

Version 2.11.4
-----------------

+ fix response without request-id

Version 2.11.3
-----------------

+ fix record repr bug

Version 2.11.2
-----------------

+ add security token

Version 2.11.1
-----------------

+ fix signature bug
+ fix null value parse error in pb mode
+ fix ClosedTime parse error in ListShardResult
+ fix failed_record_count in FailedRecordCount
+ add example
+ correct doc

Version 2.11.0
-----------------

+ complement and refactor datahub service api
+ support python 2.7, 3.4, 3.5, 3.6, pypy
+ support protobuf
