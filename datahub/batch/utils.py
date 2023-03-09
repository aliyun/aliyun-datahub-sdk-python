import struct

PADDING_BYTES = b'\x00'

def int2byte(input_int, size=4, unsigned=False):
    if size == 1:
        return struct.pack("<{}".format("B" if unsigned else "b"), input_int)
    if size == 2:
        return struct.pack("<{}".format("H" if unsigned else "h"), input_int)
    if size == 4:
        return struct.pack("<{}".format("I" if unsigned else "i"), input_int)
    if size == 8:
        return struct.pack("<{}".format("Q" if unsigned else "q"), input_int)
    return None


def byte2int(input_byte, size=4, unsigned=False):
    if size == 1:
        return struct.unpack("<{}".format("B" if unsigned else "b"), input_byte)[0]
    if size == 2:
        return struct.unpack("<{}".format("H" if unsigned else "h"), input_byte)[0]
    if size == 4:
        return struct.unpack("<{}".format("I" if unsigned else "i"), input_byte)[0]
    if size == 8:
        return struct.unpack("<{}".format("Q" if unsigned else "q"), input_byte)[0]
    return None


def float2byte(input_float):
    return struct.pack("<f", input_float)


def byte2float(input_byte):
    return struct.unpack("<f", input_byte)[0]


def double2byte(input_double):
    return struct.pack("<d", input_double)


def byte2double(input_byte):
    return struct.unpack("<d", input_byte)[0]


def bool2byte(input_bool):
    return struct.pack("<?", input_bool)


def byte2bool(input_byte):
    return struct.unpack("<?", input_byte)[0]


class SchemaObject:
    def __init__(self, project, topic, schema_register):
        self._project = project
        self._topic = topic
        self._schema_register = schema_register

    @property
    def project(self):
        return self._project

    @property
    def topic(self):
        return self._topic

    @property
    def schema_register(self):
        return self._schema_register
