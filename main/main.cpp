#include <iostream>

#include "common/util.h"
#include "network/grpc_client.h"
#include "network/network.h"

int main() {
  auto randstr = kv::common::RandString(20);
  std::cout << randstr << std::endl;
  return 0;
}
