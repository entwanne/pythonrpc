import protocol
from utils import rpc_owner, rpc_id
import serialize

def call_parent_method(self, method, args):
    server, obj = rpc_owner(self), rpc_id(self)
    args = [serialize.serialize(server, e) for e in args]
    return server.request(protocol.Command.CALL_PARENT_METHOD, (obj, method, args))

# https://docs.python.org/2/reference/datamodel.html#special-method-names
# https://docs.python.org/3/reference/datamodel.html#special-method-names
# Except descriptors and instancecheck/subclasscheck

class RPCWrapper(object):
    def __init__(self, rpc_owner, rpc_id):
        object.__setattr__(self, 'owner', rpc_owner) # RPCServer
        object.__setattr__(self, 'id', rpc_id)

    def __call__(self, *args, **kwargs):
        server, obj = rpc_owner(self), rpc_id(self)
        args = [serialize.serialize(server, e) for e in args]
        kwargs = {serialize.serialize(server, k): serialize.serialize(server, v) for (k, v) in kwargs.items()}
        return server.request(protocol.Command.CALL_OBJ, (obj, args, kwargs))

    def __repr__(self):
        return call_parent_method(self, '__repr__', ())
    def __str__(self):
        return call_parent_method(self, '__str__', ())
    def __bytes__(self): # PY3k
        return call_parent_method(self, '__bytes__', ())
    def __format__(self, fmt): # PY3k
        return call_parent_method(self, '__format__', (fmt,))
    def __lt__(self, other):
        return call_parent_method(self, '__lt__', (other,))
    def __le__(self, other):
        return call_parent_method(self, '__le__', (other,))
    def __eq__(self, other):
        return call_parent_method(self, '__eq__', (other,))
    def __ne__(self, other):
        return call_parent_method(self, '__ne__', (other,))
    def __gt__(self, other):
        return call_parent_method(self, '__gt__', (other,))
    def __ge__(self, other):
        return call_parent_method(self, '__ge__', (other,))
    def __cmp__(self, other): # PY2k
        return call_parent_method(self, '__cmp__', (other,))
    def __rcmp__(self, other): # PY2k
        return call_parent_method(self, '__rcmp__', (other,))
    def __hash__(self):
        return call_parent_method(self, '__hash__', ())
    def __bool__(self): # PY3k
        return call_parent_method(self, '__bool__', ())
    def __nonzero__(self): # PY2k
        return call_parent_method(self, '__nonzero__', ())
    def __unicode__(self): # PY2k
        return call_parent_method(self, '__unicode__', ())

    def __getattr__(self, name):
        return call_parent_method(self, '__getattr__', (name,))
    def __getattribute__(self, name):
        return call_parent_method(self, '__getattribute__', (name,))
    def __setattr__(self, name, value):
        return call_parent_method(self, '__setattr__', (name, value))
    def __delattr__(self, name):
        return call_parent_method(self, '__delattr__', (name,))
    
    def __dir__(self): # PY3k
        return call_parent_method(self, '__dir__', ())

    def __len__(self):
        return call_parent_method(self, '__len__', ())
    def __length_hint__(self): # PY3k
        return call_parent_method(self, '__length_hint__', ())
    def __getitem__(self, key):
        return call_parent_method(self, '__getitem__', (key,))
    def __missing__(self, key):
        return call_parent_method(self, '__missing__', (key,))
    def __setitem__(self, key, value):
        return call_parent_method(self, '__setitem__', (key, value))
    def __delitem__(self, key):
        return call_parent_method(self, '__delitem__', (key,))
    def __iter__(self):
        return call_parent_method(self, '__iter__', ())
    def __reversed__(self):
        return call_parent_method(self, '__reversed__', ())
    def __contains__(self, item):
        return call_parent_method(self, '__contains__', (item,))

    def __getslice__(self, i, j): # PY2k
        return call_parent_method(self, '__getslice__', (i, j))
    def __setslice__(self, i, j, seq): # PY2k
        return call_parent_method(self, '__setslice__', (i, j, seq))
    def __delslice__(self, i, j): # PY2k
        return call_parent_method(self, '__delslice__', (i, j))

    def __add__(self, other):
        return call_parent_method(self, '__add__', (other,))
    def __sub__(self, other):
        return call_parent_method(self, '__sub__', (other,))
    def __mul__(self, other):
        return call_parent_method(self, '__mul__', (other,))
    def __truediv__(self, other):
        return call_parent_method(self, '__truediv__', (other,))
    def __div__(self, other): # PY2k
        return call_parent_method(self, '__truediv__', (div,))
    def __floordiv__(self, other):
        return call_parent_method(self, '__floordiv__', (other,))
    def __mod__(self, other):
        return call_parent_method(self, '__mod__', (other,))
    def __divmod__(self, other):
        return call_parent_method(self, '__divmod__', (other,))
    def __pow__(self, *args):
        return call_parent_method(self, '__pow__', args)
    def __lshift__(self, other):
        return call_parent_method(self, '__lshift__', (other,))
    def __rshift__(self, other):
        return call_parent_method(self, '__rshift__', (other,))
    def __and__(self, other):
        return call_parent_method(self, '__and__', (other,))
    def __xor__(self, other):
        return call_parent_method(self, '__xor__', (other,))
    def __or__(self, other):
        return call_parent_method(self, '__or__', (other,))
    def __radd__(self, other):
        return call_parent_method(self, '__radd__', (other,))
    def __rsub__(self, other):
        return call_parent_method(self, '__rsub__', (other,))
    def __rmul__(self, other):
        return call_parent_method(self, '__rmul__', (other,))
    def __rtruediv__(self, other):
        return call_parent_method(self, '__rtruediv__', (other,))
    def __rfloordiv__(self, other):
        return call_parent_method(self, '__rfloordiv__', (other,))
    def __rmod__(self, other):
        return call_parent_method(self, '__rmod__', (other,))
    def __rdivmod__(self, other):
        return call_parent_method(self, '__rdivmod__', (other,))
    def __rpow__(self, other):
        return call_parent_method(self, '__rpow__', (other,))
    def __rlshift__(self, other):
        return call_parent_method(self, '__rlshift__', (other,))
    def __rrshift__(self, other):
        return call_parent_method(self, '__rrshift__', (other,))
    def __rand__(self, other):
        return call_parent_method(self, '__rand__', (other,))
    def __rxor__(self, other):
        return call_parent_method(self, '__rxor__', (other,))
    def __ror__(self, other):
        return call_parent_method(self, '__ror__', (other,))
    def __iadd__(self, other):
        return call_parent_method(self, '__iadd__', (other,))
    def __isub__(self, other):
        return call_parent_method(self, '__isub__', (other,))
    def __imul__(self, other):
        return call_parent_method(self, '__imul__', (other,))
    def __itruediv__(self, other):
        return call_parent_method(self, '__itruediv__', (other,))
    def __ifloordiv__(self, other):
        return call_parent_method(self, '__ifloordiv__', (other,))
    def __imod__(self, other):
        return call_parent_method(self, '__imod__', (other,))
    def __idivmod__(self, other):
        return call_parent_method(self, '__idivmod__', (other,))
    def __ipow__(self, *args):
        return call_parent_method(self, '__ipow__', args)
    def __ilshift__(self, other):
        return call_parent_method(self, '__ilshift__', (other,))
    def __irshift__(self, other):
        return call_parent_method(self, '__irshift__', (other,))
    def __iand__(self, other):
        return call_parent_method(self, '__iand__', (other,))
    def __ixor__(self, other):
        return call_parent_method(self, '__ixor__', (other,))
    def __ior__(self, other):
        return call_parent_method(self, '__ior__', (other,))
    def __neg__(self):
        return call_parent_method(self, '__neg__', ())
    def __pos__(self):
        return call_parent_method(self, '__pos__', ())
    def __abs__(self):
        return call_parent_method(self, '__abs__', ())
    def __invert__(self):
        return call_parent_method(self, '__invert__', ())
    def __complex__(self):
        return call_parent_method(self, '__complex__', ())
    def __int__(self):
        return call_parent_method(self, '__int__', ())
    def __long__(self): # PY2k
        return call_parent_method(self, '__long__', ())
    def __float__(self):
        return call_parent_method(self, '__float__', ())
    def __oct__(self): # PY2k
        return call_parent_method(self, '__oct__', ())
    def __hex__(self): # PY2k
        return call_parent_method(self, '__hex__', ())
    def __round__(self): # PY3k
        return call_parent_method(self, '__round__', ())
    def __index__(self):
        return call_parent_method(self, '__index__', ())
    def __corce__(self): # PY2k
        return call_parent_method(self, '__coerce__', ())

    def __enter__(self):
        return call_parent_method(self, '__enter__', ())
    def __exit__(self, exc_type, exc_value, traceback):
        return call_parent_method(self, '__exit__', (exc_type, exc_value, traceback))
