import msgpack

class MsgType:
    REQUEST = 0
    RESPONSE = 1

class Command:
    GET_OBJ = 0
    CALL_OBJ = 3
    CALL_PARENT_METHOD = 5

def pack(*args):
    return msgpack.packb(args, use_bin_type=True)

def unpack(msg):
    return msgpack.unpackb(msg, use_list=False, encoding='utf-8')
