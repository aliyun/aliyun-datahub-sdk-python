from decimal import Decimal

from .record_header import RecordHeader, RECORD_HEADER_SIZE
from .utils import *
from ..exceptions import InvalidParameterException, DatahubException
from ..models import FieldType
from ..utils.converters import to_binary, to_text

BYTE_SIZE_ONE_FIELD = 8
FIELD_COUNT_BYTE_SIZE = 4
INT_BYTE_SIZE = 4
PADDING_BYTES = b'\x00'


class BinaryRecord:
    def __init__(self, schema=None, version_id=None, buffer=None, header=None):
        self._field_cnt = len(schema.field_list) if schema else 1
        self._attr_length = 0
        self._version_id = version_id
        self._schema = schema
        self._buffer = buffer if buffer else bytes()
        self._attr_map = dict()
        self._has_init_attr_map = False

        self._null_bit = [0] * (((self._field_cnt + 63) >> 6) << 3)  # N/8
        self._fields = [bytes()] * self._field_cnt  # N*8

        self._field_pos = RECORD_HEADER_SIZE + FIELD_COUNT_BYTE_SIZE + len(self._null_bit)
        self._next_pos = self._field_pos + self._field_cnt * BYTE_SIZE_ONE_FIELD

        # RecordHeader
        self._record_header = header

    # =======================
    # put record
    # =======================

    def serialize(self):
        # record header
        record_head_byte = RecordHeader.serialize(0, self._version_id, self.__get_record_size(), self._next_pos)
        if len(record_head_byte) != RECORD_HEADER_SIZE:
            raise DatahubException("Record header size is {}, should be {}".format(len(record_head_byte), RECORD_HEADER_SIZE))
        self.__append(record_head_byte)

        # field count
        field_count_byte = int2byte(self._field_cnt)
        self.__append(field_count_byte)

        # NullBit: N/8
        for null_bit in self._null_bit:
            null_bit_byte = int2byte(null_bit, size=1, unsigned=True)
            self.__append(null_bit_byte)

        # Field: N*8
        for field_data in self._fields:
            self.__append(field_data)

        # attribute map
        attr_map_len_byte = int2byte(len(self._attr_map))
        self.__append(attr_map_len_byte)

        for key, val in self._attr_map.items():
            key_len_byte = int2byte(len(key))
            self.__append(key_len_byte)
            key_byte = to_binary(key)
            self.__append(key_byte)
            val_len_byte = int2byte(len(val))
            self.__append(val_len_byte)
            val_byte = to_binary(val)
            self.__append(val_byte)
        return self._buffer

    def set_field(self, pos, value):
        self.__set_none(pos)
        if self._schema is None:    # Blob
            if not isinstance(value, bytes):
                raise InvalidParameterException("Only support write bytes for no schema")
            self._fields[0] = self.__set_byte_field(value)
        else:                       # Tuple
            field = self._schema.get_field(pos)
            field_type = field.type
            if field_type is FieldType.STRING or field_type is FieldType.DECIMAL:
                value_byte = self.__set_byte_field(to_binary(str(value)))
            elif field_type is FieldType.BOOLEAN:
                value_byte = bool2byte(value)
                value_byte += PADDING_BYTES * 7
            elif field_type is FieldType.FLOAT:
                value_byte = float2byte(value)
                value_byte += PADDING_BYTES * 4
            elif field_type is FieldType.DOUBLE:
                value_byte = double2byte(value)
            elif field_type in (FieldType.TINYINT, FieldType.SMALLINT, FieldType.INTEGER,
                                FieldType.BIGINT, FieldType.TIMESTAMP):
                value_byte = int2byte(value, size=8)
            else:
                raise DatahubException("Error field type. {}".format(field_type))
            self._fields[pos] = value_byte

    def add_attribute(self, key, value):
        self._attr_map[key] = value
        self._attr_length += (INT_BYTE_SIZE * 2 + len(key) + len(value))
        self._has_init_attr_map = False

    def __set_byte_field(self, value):
        value_byte_len = len(value)
        if value_byte_len <= 7:
            value += (PADDING_BYTES * (7 - value_byte_len))
            value += int2byte(value_byte_len | 0x80, size=1, unsigned=True)
            return value
        # offset from the end of RecordHeader
        value_offset = FIELD_COUNT_BYTE_SIZE + len(self._null_bit) + self._field_cnt * 8
        for i in range(self._field_cnt, len(self._fields)):
            value_offset += len(self._fields[i])
        # length(4 Byte) + offset(4 Byte)
        value_byte = int2byte(value_byte_len, size=4) + int2byte(value_offset, size=4)
        # add padding to 8N Byte
        value += (PADDING_BYTES * (BYTE_SIZE_ONE_FIELD - (len(value) % BYTE_SIZE_ONE_FIELD)))
        self._fields.append(value)
        self._next_pos += len(value)
        return value_byte

    def __append(self, src_buffer):
        self._buffer += src_buffer

    def __set_none(self, pos, none=False):
        if not none:
            self.__check_pos_valid(pos)
            index = pos >> 3
            self._null_bit[index] |= (1 << (pos & 0x07))

    # =======================
    # get record
    # =======================

    @classmethod
    def deserialize(cls, schema, buffer, record_header):
        binary_record = cls(schema, record_header.schema_version, buffer, record_header)

        # Deserialize null_bit
        null_bit_pos = RECORD_HEADER_SIZE + FIELD_COUNT_BYTE_SIZE
        for index in range(len(binary_record._null_bit)):
            binary_record._null_bit[index] = byte2int(binary_record._buffer[null_bit_pos:null_bit_pos + 1], size=1)
            null_bit_pos += 1

        # Deserialize filed
        for i in range(binary_record._field_cnt):
            binary_record._fields[i] = binary_record.__get_field(i)

        # Deserialize attribute map
        binary_record.__init_attr_map_if_need()

        return binary_record

    def get_field(self, pos):
        self.__check_pos_valid(pos)
        return self._fields[pos]

    def get_attribute(self):
        return self._attr_map

    def __get_field(self, pos):
        if self.__is_field_none(pos):
            return None
        field_byte = self.__read_field(pos)
        if self._schema is None:    # BLOB
            return self.__get_byte_field(field_byte)
        else:                       # TUPLE
            field_type = self._schema.get_field(pos).type
            value = None
            # length = 1
            if field_type is FieldType.BOOLEAN:
                value = byte2bool(field_byte[:1])
            elif field_type is FieldType.TINYINT:
                value = byte2int(field_byte[:1], size=1)
            # length = 2
            elif field_type is FieldType.SMALLINT:
                value = byte2int(field_byte[:2], size=2)
            # length = 4
            elif field_type is FieldType.INTEGER:
                value = byte2int(field_byte[:4], size=4)
            elif field_type is FieldType.FLOAT:
                value = byte2float(field_byte[:4])
            # length = 8
            elif field_type is FieldType.BIGINT or field_type is FieldType.TIMESTAMP:
                value = byte2int(field_byte, size=8)
            elif field_type is FieldType.DOUBLE:
                value = byte2double(field_byte)
            # length = ?
            elif field_type is FieldType.STRING or field_type is FieldType.DECIMAL:
                value = self.__get_str_field(field_byte)
                if field_type is FieldType.DECIMAL:
                    value = Decimal(value)
        return value

    def __get_byte_field(self, value_byte):
        data = byte2int(value_byte, size=8)
        is_little_str = (data & (0x80 << 56)) != 0
        if is_little_str:
            str_size = ((data >> 56) & 0x07)
            value = value_byte[:str_size]
        else:
            str_offset = RECORD_HEADER_SIZE + (data >> 32)
            str_size = byte2int(value_byte[:4], size=4)
            value = self._buffer[str_offset:str_offset + str_size]
            self._next_pos -= str_size
        return value

    def __get_str_field(self, value_byte):
        return to_text(self.__get_byte_field(value_byte))

    def __is_field_none(self, pos):
        self.__check_pos_valid(pos)
        index = pos >> 3
        value = self._null_bit[index] & (1 << (pos & 0x07))
        return value == 0

    def __init_attr_map_if_need(self):
        if self._has_init_attr_map:
            return
        offset = self._record_header.attr_offset
        attr_size = byte2int(self._buffer[offset:offset+4])
        if attr_size != 0 and self._attr_map is None:
            self._attr_map = dict()

        offset += INT_BYTE_SIZE
        for i in range(attr_size):
            key_size = byte2int(self._buffer[offset:offset+4])
            offset += INT_BYTE_SIZE
            key_str = to_text(self._buffer[offset:offset+key_size])
            offset += key_size
            val_size = byte2int(self._buffer[offset:offset+4])
            offset += INT_BYTE_SIZE
            value_str = to_text(self._buffer[offset:offset+val_size])
            offset += val_size
            self._attr_map[key_str] = value_str
            self._attr_length += (key_size + val_size + 2*INT_BYTE_SIZE)
        self._has_init_attr_map = True

    def __get_field_offset(self, pos):
        return self._field_pos + pos * BYTE_SIZE_ONE_FIELD

    def __read_field(self, pos):
        offset = self.__get_field_offset(pos)
        return self._buffer[offset:offset + BYTE_SIZE_ONE_FIELD]

    # =======================
    # common
    # =======================

    def __get_record_size(self):
        return INT_BYTE_SIZE + self._attr_length + self._next_pos

    def __check_pos_valid(self, pos):
        if pos < 0 or pos >= self._field_cnt:
            raise InvalidParameterException("Invalid position. position: {}, fieldCount: {}".format(pos, self._field_cnt))

    @property
    def field_cnt(self):
        return self._field_cnt

    @property
    def version_id(self):
        return self._version_id

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema
