import uuid
import threading

import zmq

import protocol
import serialize
from worker import RPCWorkerPool, RPCWorker


class RPCServer(object):
    def __init__(self, socket_uri, bind=False, nb_workers=0, nb_max_workers=None):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PAIR)
        if bind:
            self.socket.bind(socket_uri)
        else:
            self.socket.connect(socket_uri)
        self.objects = {}
        self.resps = {}
        self.events = {}
        self.wpool = RPCWorkerPool(self, nb_workers, nb_max_workers)
        self.listener = RPCServerListener(self)

    def start(self):
        self.wpool.start()
        self.listener.start()

    def stop(self):
        for e in self.events.values():
            e.set()
        self.listener.stop()
        self.wpool.stop()

    def register(self, id, obj):
        self.objects[id] = obj

    def wait_for_resp(self, msg_id):
        if not msg_id in self.resps:
            event = threading.Event()
            self.events[msg_id] = event
            if not msg_id in self.resps: # If msg occurred between first test and instanciation of the event
                event.wait()
            del self.events[msg_id]
        msg = self.resps[msg_id]
        del self.resps[msg_id]
        return msg

    def request(self, cmd, args):
        msg_id = uuid.uuid4().hex
        self.socket.send(protocol.pack(msg_id, protocol.MsgType.REQUEST, (cmd, args)))
        return serialize.unserialize(self, self.wait_for_resp(msg_id))

    def answer(self, msg_id, msg):
        self.socket.send(protocol.pack(msg_id, protocol.MsgType.RESPONSE, msg))

    def get(self, id):
        return self.request(protocol.Command.GET_OBJ, (id,))


class RPCServerListener(threading.Thread):
    def __init__(self, server):
        super(RPCServerListener, self).__init__()
        self.server = server
        self.running = True

    def run(self):
        while self.running:
            if self.server.socket.poll(100) == zmq.POLLIN:
                msg_id, msg_type, msg = protocol.unpack(self.server.socket.recv())
                if msg_type == protocol.MsgType.REQUEST:
                    #print('Got request', msg)
                    #w = RPCWorker(self.server, msg_id, msg)
                    #w.start()
                    self.server.wpool.push(msg_id, msg)
                elif msg_type == protocol.MsgType.RESPONSE:
                    #print('Got response', msg)
                    self.server.resps[msg_id] = msg # Use the lock
                    if msg_id in self.server.events:
                        self.server.events[msg_id].set()

    def stop(self):
        self.running = False
