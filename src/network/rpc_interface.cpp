#include "network/rpc_interface.h"

#include "common/exception.h"
#include "network/grpc_client.h"

namespace kv::network {

RPCInterface *instance = nullptr;

RPCInterface *GetRPCInterface() { return nullptr; }

void SetRPCInterface(RPCInterface *rpc) {}

}  // namespace kv::network
