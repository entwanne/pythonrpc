def rpc_owner(rpc):
    return object.__getattribute__(rpc, 'owner')

def rpc_id(rpc):
    return object.__getattribute__(rpc, 'id')
