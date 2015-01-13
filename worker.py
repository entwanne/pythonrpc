import threading

import protocol
import serialize

class RPCWorker(threading.Thread):
    def __init__(self, server, msg_id, request):
        super().__init__()
        self.server = server
        self.msg_id = msg_id
        self.request = request

    def getobj(self, obj_id):
        return self.server.objects.get(obj_id)
        
    def run(self):
        #print('RPCWorker launched with', self.request)
        cmd, args = self.request
        if cmd == protocol.Command.GET_OBJ:
            obj_id, = args
            r = self.getobj(obj_id)
        elif cmd == protocol.Command.CALL_OBJ:
            obj_id, fargs, fkwargs = args
            fargs = [serialize.unserialize(self.server, e) for e in fargs]
            fkwargs = {serialize.unserialize(self.server, k): serialize.unserialize(self.server, v) for (k, v) in fkwargs}
            r = self.getobj(obj_id)(*fargs, **fkwargs)
        elif cmd == protocol.Command.CALL_PARENT_METHOD:
            obj_id, name, fargs = args
            fargs = [serialize.unserialize(self.server, e) for e in fargs]
            obj = self.getobj(obj_id)
            method = getattr(type(obj), name)
            r = method(obj, *fargs)
        self.server.answer(self.msg_id, serialize.serialize(self.server, r))
