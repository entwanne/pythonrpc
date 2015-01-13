import threading

import protocol
import serialize
from exceptions import Exceptions

class RPCWorker(threading.Thread):
    def __init__(self, server, msg_id, request):
        super(RPCWorker, self).__init__()
        self.server = server
        self.msg_id = msg_id
        self.request = request

    def getobj(self, obj_id):
        return self.server.objects.get(obj_id)
        
    def run(self):
        #print('RPCWorker launched with', self.request)
        cmd, args = self.request
        try:
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
            msg = serialize.serialize(self.server, r)
        except BaseException as e:
            num = Exceptions.get(type(e))
            if num is None:
                raise
            msg = serialize.RPCType.EXCEPTION, (num, e.args)
        self.server.answer(self.msg_id, msg)
