from server import RPCServer
import protocol

s = RPCServer('tcp://127.0.0.1:5000')
s.start()

A = s.get('A')
print(A)
print(A.a)
print(object.__repr__(A.a))
A.a = [1, 2, 3]
print(A.a)
print(object.__repr__(A.a))

a = A()
print(a)
print(a.a)
print(a.a[-1])

tutu = s.get('tutu')
l = tutu(lambda x: x * 2)
print(l)

s.stop()