import threading

import protocol
import serialize
from exceptions import Exceptions

class RPCWorkerPool(object):
    def __init__(self, server, nb_workers, nb_max_workers):
        self.server = server
        self.workers = set()
        if nb_max_workers is None:
            self.max_workers = None
        else:
            self.max_workers = max(nb_workers, nb_max_workers)
        for _ in range(nb_workers):
            RPCWorker(self.server, self.workers, True)

    def start(self):
        for w in self.workers:
            w.start()

    def stop(self):
        workers = list(self.workers)
        for w in self.workers:
            w.stop()

    def push(self, msg_id, msg):
        if self.workers:
            w = min(self.workers, key=lambda w: w.busy)
            if w.busy:
                if len(self.workers) == self.max_workers:
                    raise ValueError("All workers are busy")
            else:
                print('reuse')
                w.push(msg_id, msg)
                return
        print('new worker')
        w = RPCWorker(self.server, self.workers)
        w.push(msg_id, msg)
        w.start()
            

class RPCWorker(threading.Thread):
    def __init__(self, server, workers, persistent=False):
        super(RPCWorker, self).__init__()
        self.server = server
        self.workers = workers
        self.commands = {
            protocol.Command.GET_OBJ: self.cmd_get_obj,
            protocol.Command.CALL_OBJ: self.cmd_call_obj,
            protocol.Command.CALL_PARENT_METHOD: self.cmd_call_parent_method
        }
        self.persistent = persistent
        self.sem = threading.Semaphore(0)
        self.busy_lock = threading.Lock()
        self.workers.add(self)

    @property
    def busy(self):
        r = self.busy_lock.acquire(False)
        if r:
            self.busy_lock.release()
        return not r

    def getobj(self, obj_id):
        return self.server.objects.get(obj_id)

    def push(self, msg_id, request):
        if not self.busy_lock.acquire(False):
            raise ValueError("Worker is busy")
        self.msg_id = msg_id
        self.request = request
        self.sem.release()

    def cmd_get_obj(self, obj_id):
        return self.getobj(obj_id)

    def cmd_call_obj(self, obj_id, args, kwargs):
        args = [serialize.unserialize(self.server, e) for e in args]
        kwargs = {serialize.unserialize(self.server, k): serialize.unserialize(self.server, v) for (k, v) in kwargs.items()}
        return self.getobj(obj_id)(*args, **kwargs)

    def cmd_call_parent_method(self, obj_id, name, args):
        args = [serialize.unserialize(self.server, e) for e in args]
        obj = self.getobj(obj_id)
        method = getattr(type(obj), name)
        return method(obj, *args)

    def run(self):
        running = True
        while running:
            running = self.persistent
            self.sem.acquire()
            if self.msg_id is None:
                break
            cmd, args = self.request
            try:
                msg = serialize.serialize(self.server,
                                          self.commands[cmd](*args))
            except BaseException as e:
                num = Exceptions.get(type(e))
                if num is None:
                    raise
                msg = serialize.RPCType.EXCEPTION, (num, e.args)
            self.server.answer(self.msg_id, msg)
            self.busy_lock.release()
        self.workers.remove(self)

    def stop(self):
        self.push(None, None)
