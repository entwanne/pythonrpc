import uuid
import threading
import zmq

import protocol

def call_parent_method(self, method, args):
    server, obj = rpc_owner(self), rpc_id(self)
    args = [serialize(server, e) for e in args]
    return server.request(protocol.Command.CALL_PARENT_METHOD, (obj, method, args))

class RPCWrapper:
    def __init__(self, rpc_owner, rpc_id):
        object.__setattr__(self, 'owner', rpc_owner) # RPCServer
        object.__setattr__(self, 'id', rpc_id)

    def __getattr__(self, name):
        server, obj = rpc_owner(self), rpc_id(self)
        return server.request(protocol.Command.OBJ_GET_ATTR, (obj, name))

    def __getattribute__(self, name):
        server, obj = rpc_owner(self), rpc_id(self)
        return server.request(protocol.Command.OBJ_GET_ATTR, (obj, name))

    def __setattr__(self, name, value):
        server, obj = rpc_owner(self), rpc_id(self)
        return server.request(protocol.Command.OBJ_SET_ATTR, (obj, name, serialize(server, value)))

    def __call__(self, *args, **kwargs):
        server, obj = rpc_owner(self), rpc_id(self)
        args = [serialize(server, e) for e in args]
        kwargs = {serialize(server, k): serialize(server, v) for (k, v) in kwargs.items()}
        return server.request(protocol.Command.CALL_OBJ, (obj, args, kwargs))

    def __str__(self):
        return call_parent_method(self, '__str__', ())
    def __mul__(self, rhs):
        return call_parent_method(self, '__mul__', (rhs,))
    def __rmul__(self, rhs):
        return call_parent_method(self, '__rmul__', (rhs,))


def rpc_owner(rpc):
    return object.__getattribute__(rpc, 'owner')

def rpc_id(rpc):
    return object.__getattribute__(rpc, 'id')

class RPCType:
    PRIMITIVE = 0
    REMOTE_OBJ = 1
    LOCAL_OBJ = 2
        
def serialize(server, obj):
    if type(obj) in {type(None), bool, int, float, str}:
        return RPCType.PRIMITIVE, obj
    if type(obj) is tuple:
        return RPCType.PRIMITIVE, tuple(serialize(server, e) for e in obj)
    if isinstance(obj, RPCWrapper):
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
        return RPCWrapper(server, obj)

class RPCWorker(threading.Thread):
    def __init__(self, server, msg_id, request):
        super().__init__()
        self.server = server
        self.msg_id = msg_id
        self.request = request

    def getobj(self, obj_id):
        return self.server.objects.get(obj_id)
        
    def run(self):
        print('RPCWorker launched with', self.request)
        cmd, args = self.request
        if cmd == protocol.Command.GET_OBJ:
            obj_id, = args
            r = self.getobj(obj_id)
        elif cmd == protocol.Command.OBJ_GET_ATTR:
            obj_id, name = args
            r = getattr(self.getobj(obj_id), name)
        elif cmd == protocol.Command.OBJ_SET_ATTR:
            obj_id, name, value = args
            r = setattr(self.getobj(obj_id), name, unserialize(self.server, value))
        elif cmd == protocol.Command.CALL_OBJ:
            obj_id, fargs, fkwargs = args
            fargs = [unserialize(self.server, e) for e in fargs]
            fkwargs = {unserialize(self.server, k): unserialize(self.server, v) for (k, v) in fkwargs}
            r = self.getobj(obj_id)(*fargs, **fkwargs)
        elif cmd == protocol.Command.CALL_PARENT_METHOD:
            obj_id, name, fargs = args
            fargs = [unserialize(self.server, e) for e in fargs]
            obj = self.getobj(obj_id)
            method = getattr(type(obj), name)
            r = method(obj, *fargs)
        self.server.answer(self.msg_id, serialize(self.server, r))

class RPCServer:
    def __init__(self, socket_uri, bind=False):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PAIR)
        if bind:
            self.socket.bind(socket_uri)
        else:
            self.socket.connect(socket_uri)
        self.objects = {}
        self.resps = {}
        self.events = {}

    def start(self):
        self.listener = RPCServerListener(self)
        self.listener.start()

    def stop(self):
        for e in self.events.values():
            e.set()
        self.listener.stop()

    def register(self, id, obj):
        self.objects[id] = obj

    def wait_for_resp(self, msg_id):
        if not msg_id in self.resps:
            event = threading.Event()
            self.events[msg_id] = event
            if not msg_id in self.resps:
                event.wait()
            del self.events[msg_id]
        msg = self.resps[msg_id]
        del self.resps[msg_id]
        return msg

    def request(self, cmd, args):
        msg_id = uuid.uuid4().hex
        self.socket.send(protocol.pack(msg_id, protocol.MsgType.REQUEST, (cmd, args)))
        return unserialize(self, self.wait_for_resp(msg_id))

    def answer(self, msg_id, msg):
        self.socket.send(protocol.pack(msg_id, protocol.MsgType.RESPONSE, msg))

    def get(self, id):
        return self.request(protocol.Command.GET_OBJ, (id,))

class RPCServerListener(threading.Thread):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self.running = True

    def run(self):
        while self.running:
            if self.server.socket.poll(0.1) == zmq.POLLIN:
                msg_id, msg_type, msg = protocol.unpack(self.server.socket.recv())
                if msg_type == protocol.MsgType.REQUEST:
                    print('Got request', msg)
                    w = RPCWorker(self.server, msg_id, msg)
                    w.start()
                elif msg_type == protocol.MsgType.RESPONSE:
                    print('Got response', msg)
                    self.server.resps[msg_id] = msg # Use the lock
                    if msg_id in self.server.events:
                        self.server.events[msg_id].set()

    def stop(self):
        self.running = False
