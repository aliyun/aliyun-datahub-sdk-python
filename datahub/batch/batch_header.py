from .utils import *
from ..utils.converters import to_binary, to_text
from ..exceptions import DatahubException, InvalidParameterException

MAGIC_NUMBER = "DHUB"
BATCH_HEAD_SIZE = 26


class BatchHeader:
    """
    Batch binary header
    """

    def __init__(self, version=None, length=None, raw_size=None, crc32=None, attributes=None, record_count=None):
        self._magic = MAGIC_NUMBER
        self._version = version
        self._length = length
        self._raw_size = raw_size
        self._crc32 = crc32
        self._attributes = attributes
        self._record_count = record_count

    @staticmethod
    def serialize(version, length, raw_size, crc32, attributes, record_count):
        header = bytes()
        header += to_binary(MAGIC_NUMBER)
        header += int2byte(version, size=4)
        header += int2byte(length, size=4, unsigned=True)
        header += int2byte(raw_size, size=4, unsigned=True)
        header += int2byte(crc32, size=4, unsigned=True)
        header += int2byte(attributes, size=2, unsigned=True)  # short
        header += int2byte(record_count, size=4, unsigned=True)
        return header

    @staticmethod
    def deserialize(header):
        if len(header) != BATCH_HEAD_SIZE:
            raise DatahubException("Batch header length is {}, should be {}".format(len(header), BATCH_HEAD_SIZE))
        if to_text(header[:4]) != MAGIC_NUMBER:
            raise InvalidParameterException("Error. Batch header should start with ", MAGIC_NUMBER)

        return BatchHeader(
            byte2int(header[4:8]),
            byte2int(header[8:12], unsigned=True),
            byte2int(header[12:16], unsigned=True),
            byte2int(header[16:20], unsigned=True),
            byte2int(header[20:22], size=2, unsigned=True),
            byte2int(header[22:26], unsigned=True)
        )

    @property
    def magic(self):
        return self._magic

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def length(self):
        return self._length

    @length.setter
    def length(self, length):
        self._length = length

    @property
    def raw_size(self):
        return self._raw_size

    @raw_size.setter
    def raw_size(self, raw_size):
        self._raw_size = raw_size

    @property
    def crc32(self):
        return self._crc32

    @crc32.setter
    def crc32(self, crc32):
        self._crc32 = crc32

    @property
    def attributes(self):
        return self._attributes

    @attributes.setter
    def attributes(self, attributes):
        self._attributes = attributes

    @property
    def record_count(self):
        return self._record_count

    @record_count.setter
    def record_count(self, record_count):
        self._record_count = record_count
