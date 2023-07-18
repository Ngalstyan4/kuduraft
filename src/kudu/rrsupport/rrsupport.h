#pragma once

#include "airreplay/airreplay.h"

namespace kudu {
namespace rrsupport {
enum KuduMsgKinds {
  kOutboundRequest = 9,
  kOutboundResponse = 10,
  kInboundRequest = 11,
  kInboundResponse = 12,
};


extern std::mutex mockCallbackerMutex;
extern std::map<std::string, std::function<void()>> mockCallbacker;

}
}  // namespace kudu