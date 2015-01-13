import wrapper
from utils import rpc_id
from exceptions import Exceptions

class RPCType:
    PRIMITIVE = 0
    REMOTE_OBJ = 1
    LOCAL_OBJ = 2
    EXCEPTION = 3
        
def serialize(server, obj):
    if type(obj) in {type(None), bool, int, float, str}:
        return RPCType.PRIMITIVE, obj
    if type(obj) is tuple:
        return RPCType.PRIMITIVE, tuple(serialize(server, e) for e in obj)
    if type(obj) is wrapper.RPCWrapper:
        return RPCType.LOCAL_OBJ, rpc_id(obj)
    rpc = id(obj)
    if rpc not in server.objects:
        server.register(rpc, obj)
    return RPCType.REMOTE_OBJ, rpc

def unserialize(server, tobj):
    t, obj = tobj
    if t == RPCType.PRIMITIVE:
        return obj
    elif t == RPCType.LOCAL_OBJ:
        return server.objects.get(obj)
    elif t == RPCType.REMOTE_OBJ:
        return wrapper.RPCWrapper(server, obj)
    elif t == RPCType.EXCEPTION:
        n, args = obj
        Exceptions.throw(n, args)
