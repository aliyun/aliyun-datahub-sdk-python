import threading
from ..exceptions import ResourceNotFoundException
from ..models import RecordSchema


class SchemaRegistryClient:
    def __init__(self, datahub_client):
        self._datahub_client = datahub_client
        self._cache = dict()
        self._cache_lock = threading.Lock()
        self._lock = threading.Lock()

    def get_version_id(self, project_name, topic_name, schema):
        schema_meta = self.__get_schema_meta(project_name, topic_name)

        try:
            result = schema_meta.get_version_id(schema)
        except ResourceNotFoundException:
            self.__update(project_name, topic_name, schema_meta)
            result = schema_meta.get_version_id(schema)
        except Exception as e:
            raise ResourceNotFoundException("VersionId not found. project: {}, topic: {}, schema: {}. {}"
                                            .format(project_name, topic_name, schema.to_json_string(), e))
        return result

    def get_schema(self, project_name, topic_name, version_id):
        schema_meta = self.__get_schema_meta(project_name, topic_name)

        try:
            result = schema_meta.get_schema(version_id)
        except ResourceNotFoundException:
            self.__update(project_name, topic_name, schema_meta)
            result = schema_meta.get_schema(version_id)
        except Exception as e:
            raise ResourceNotFoundException("Schema not found. project: {}, topic: {}, version_id: {}. {}"
                                            .format(project_name, topic_name, version_id, e))
        return result

    def __get_schema_meta(self, project_name, topic_name):
        key = self.__gen_key(project_name, topic_name)
        if key not in self._cache:
            with self._cache_lock:
                if key not in self._cache:
                    self._cache[key] = SchemaMeta()
        return self._cache.get(key)

    def __update(self, project_name, topic_name, schema_meta):
        with self._lock:
            page_num, page_size = 0, 100
            while True:
                page_num += 1
                list_schema = self._datahub_client.list_topic_schema(project_name, topic_name, page_num, page_size)
                for record_schema in list_schema.record_schema_list:
                    schema_meta.add(record_schema.get("VersionId"), RecordSchema.from_json_str(record_schema.get("RecordSchema")))
                if page_num >= list_schema.page_count:
                    break

    def __gen_key(self, project_name, topic_name):
        return "{}/{}".format(project_name, topic_name)


class SchemaMeta:
    def __init__(self):
        self._version_schema_map = dict()          # version_string --> schema_string
        self._schema_version_map = dict()          # schema_string --> version_string
        self._version_lock = threading.Lock()
        self._schema_lock = threading.Lock()

    def get_schema(self, version_id):
        with self._schema_lock:
            if version_id == -1:
                if len(self._version_schema_map) == 0:
                    raise ResourceNotFoundException("VersionSchemaMap is empty. version_id = -1")
                schema_str = list(iter(self._version_schema_map.values()))[-1]
            else:
                schema_str = self._version_schema_map.get(version_id)
            if schema_str is None:
                raise ResourceNotFoundException("Schema not found with the specified version_id {}.".format(version_id))
            return RecordSchema.from_json_str(schema_str)

    def get_version_id(self, schema):
        with self._version_lock:
            version_id = self._schema_version_map.get(schema.to_json_string())
            if version_id is None:
                raise ResourceNotFoundException("VersionId not found with the specified schema {}.".format(schema.to_json_string()))
            return version_id

    def add(self, version_id, schema):
        schema_str = schema.to_json_string()
        self._version_schema_map[version_id] = schema_str
        self._schema_version_map[schema_str] = version_id
