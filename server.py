from rpc import RPCServer

s = RPCServer('tcp://127.0.0.1:5000', bind=True)

class A:
    a = [42]
s.register('A', A)

def tutu(f):
    return f(4)
s.register('tutu', tutu)

s.start()

try:
    while True:
        pass
except KeyboardInterrupt:
    s.stop()
