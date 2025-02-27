// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <ifaddrs.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <functional>
#include <memory>
#include <ostream>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/functional/hash/hash.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/trace.h"

// Mac OS 10.9 does not appear to define HOST_NAME_MAX in unistd.h
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 64
#endif

DEFINE_bool(
    fail_dns_resolution,
    false,
    "Whether to fail all dns resolution, for tests.");
TAG_FLAG(fail_dns_resolution, hidden);

using std::function;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

using AddrInfo = unique_ptr<addrinfo, function<void(addrinfo*)>>;

// An utility wrapper around getaddrinfo() call to convert the return code
// of the libc library function into Status.
Status GetAddrInfo(
    const string& hostname,
    const addrinfo& hints,
    const string& op_description,
    AddrInfo* info) {
  addrinfo* res = nullptr;
  const int rc = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
  const int err = errno; // preserving the errno from the getaddrinfo() call
  AddrInfo result(res, ::freeaddrinfo);
  if (rc == 0) {
    if (info != nullptr) {
      info->swap(result);
    }
    return Status::OK();
  }
  const string err_msg = Substitute("unable to $0", op_description);
  if (rc == EAI_SYSTEM) {
    return Status::NetworkError(err_msg, ErrnoToString(err), err);
  }
  return Status::NetworkError(err_msg, gai_strerror(rc));
}

} // anonymous namespace

HostPort::HostPort() : host_(""), port_(0) {}

HostPort::HostPort(std::string host, uint16_t port)
    : host_(std::move(host)), port_(port) {}

HostPort::HostPort(const Sockaddr& addr)
    : host_(addr.host()), port_(addr.port()) {}

bool operator==(const HostPort& hp1, const HostPort& hp2) {
  return hp1.port() == hp2.port() && hp1.host() == hp2.host();
}

size_t HostPort::HashCode() const {
  size_t seed = 0;
  boost::hash_combine(seed, host_);
  boost::hash_combine(seed, port_);
  return seed;
}

bool HostPort::IsHostIPV6Address() const {
  if (!Initialized())
    return false;

  int num_colons = strcount(host_, ':');
  return (num_colons > 1);
}

Status HostPort::ParseString(const string& str, uint16_t default_port) {
  /*
    The `str` param is supposed to take the following combinations:
    a) Host:Port
    b) Host

    Host could be a hostname (sub0.d0.xyz.com) or an IPv6 address.
    If it is an IPv6 address, it should be wrapped around square brackets.

    According to RFC 3986, [2001:db8:1f70::999:de8:7648:6e8]:100
    is the format used for specifying IPv6 addresses with host-port.
  */

  int num_colons = strcount(str, ':');
  bool ipv6_addr = num_colons > 1;
  bool has_port = false;

  string host, port_str;

  if (ipv6_addr) {
    std::pair<string, string> p =
        strings::Split(str, strings::delimiter::Limit("]", 1));
    host = std::move(p.first);
    StripWhiteSpace(&host);
    host = StripPrefixString(host, "[");

    port_str = std::move(p.second);
    has_port = strcount(port_str, ':') > 0;
    if (has_port) {
      StripWhiteSpace(&port_str);
      port_str = StripPrefixString(port_str, ":");
    }
  } else {
    has_port = num_colons == 1;
    std::pair<string, string> p =
        strings::Split(str, strings::delimiter::Limit(":", 1));
    host = std::move(p.first);
    StripWhiteSpace(&host);
    port_str = std::move(p.second);
  }

  // Parse the port.
  uint32_t port;
  if (!has_port) {
    // No port specified.
    port = default_port;
  } else if (!SimpleAtoi(port_str, &port) || port > 65535) {
    return Status::InvalidArgument("Invalid port", str);
  }

  host_.swap(host);
  port_ = port;
  return Status::OK();
}

Status HostPort::ResolveAddresses(vector<Sockaddr>* addresses) const {
  TRACE_EVENT1("net", "HostPort::ResolveAddresses", "host", host_);
  TRACE_COUNTER_SCOPE_LATENCY_US("dns_us");
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET6;
  hints.ai_socktype = SOCK_STREAM;

  // IPv4 addresses are returned, only if the local system has at least one IPv4
  // address configured and IPv6 addresses are returned only if the local system
  // has at least one IPv6 address configured. The loopback address is not
  // considered for this case as valid as a configured address.
  // Ref: http://man7.org/linux/man-pages/man3/getaddrinfo.3.html
  hints.ai_flags = AI_ADDRCONFIG;

  AddrInfo result;
  const string op_description = Substitute("resolve address for $0", host_);
  LOG_SLOW_EXECUTION(WARNING, 200, op_description) {
    RETURN_NOT_OK(GetAddrInfo(host_, hints, op_description, &result));
  }
  for (const addrinfo* ai = result.get(); ai != nullptr; ai = ai->ai_next) {
    CHECK_EQ(ai->ai_family, AF_INET6);
    struct sockaddr_in6* addr =
        reinterpret_cast<struct sockaddr_in6*>(ai->ai_addr);
    addr->sin6_port = htons(port_);
    Sockaddr sockaddr(*addr);
    if (addresses) {
      addresses->push_back(sockaddr);
    }
    VLOG(2) << "Resolved address " << sockaddr.ToString() << " for host/port "
            << ToString();
  }
  if (PREDICT_FALSE(FLAGS_fail_dns_resolution)) {
    return Status::NetworkError("injected DNS resolution failure");
  }
  return Status::OK();
}

Status HostPort::ParseStrings(
    const string& comma_sep_addrs,
    uint16_t default_port,
    vector<HostPort>* res) {
  vector<string> addr_strings =
      strings::Split(comma_sep_addrs, ",", strings::SkipEmpty());
  for (const string& addr_string : addr_strings) {
    HostPort host_port;
    RETURN_NOT_OK(host_port.ParseString(addr_string, default_port));
    res->push_back(host_port);
  }
  return Status::OK();
}

string HostPort::ToString() const {
  // to be compatible with RFC 3986, [2001:db8:1f70::999:de8:7648:6e8]:100
  // style of host:port
  return IsHostIPV6Address() ? Substitute("[$0]:$1", host_, port_)
                             : Substitute("$0:$1", host_, port_);
}

string HostPort::ToCommaSeparatedString(const vector<HostPort>& hostports) {
  vector<string> hostport_strs;
  for (const HostPort& hostport : hostports) {
    hostport_strs.push_back(hostport.ToString());
  }
  return JoinStrings(hostport_strs, ",");
}

Network::Network() : addr_(0), netmask_(0) {}

Network::Network(uint128 addr, uint128 netmask)
    : addr_(addr), netmask_(netmask) {}

bool Network::WithinNetwork(const Sockaddr& addr) const {
  return (NetworkByteOrder::Load128(addr.addr().sin6_addr.s6_addr) &
          netmask_) == (addr_ & netmask_);
}

Status Network::ParseCIDRString(const string& addr) {
  std::pair<string, string> p =
      strings::Split(addr, strings::delimiter::Limit("/", 1));

  kudu::Sockaddr sockaddr;
  Status s = sockaddr.ParseString(p.first, 0);

  uint32_t bits;
  bool success = SimpleAtoi(p.second, &bits);

  if (!s.ok() || !success || bits > 128) {
    return Status::NetworkError("Unable to parse CIDR address", addr);
  }

  // Netmask in network byte order
  uint128 netmask(0xffffffffffffffff, 0xffffffffffffffff);
  netmask >>= bits;
  netmask = ~netmask;

  addr_ = NetworkByteOrder::Load128(sockaddr.addr().sin6_addr.s6_addr);
  netmask_ = netmask;
  return Status::OK();
}

Status Network::ParseCIDRStrings(
    const string& comma_sep_addrs,
    vector<Network>* res) {
  vector<string> addr_strings =
      strings::Split(comma_sep_addrs, ",", strings::SkipEmpty());
  for (const string& addr_string : addr_strings) {
    Network network;
    RETURN_NOT_OK(network.ParseCIDRString(addr_string));
    res->push_back(network);
  }
  return Status::OK();
}

bool IsPrivilegedPort(uint16_t port) {
  return port <= 1024 && port != 0;
}

Status ParseAddressList(
    const std::string& addr_list,
    uint16_t default_port,
    std::vector<Sockaddr>* addresses) {
  vector<HostPort> host_ports;
  RETURN_NOT_OK(HostPort::ParseStrings(addr_list, default_port, &host_ports));
  if (host_ports.empty())
    return Status::InvalidArgument("No address specified");
  unordered_set<Sockaddr> uniqued;
  for (const HostPort& host_port : host_ports) {
    vector<Sockaddr> this_addresses;
    RETURN_NOT_OK(host_port.ResolveAddresses(&this_addresses));

    // Only add the unique ones -- the user may have specified
    // some IP addresses in multiple ways
    for (const Sockaddr& addr : this_addresses) {
      if (InsertIfNotPresent(&uniqued, addr)) {
        addresses->push_back(addr);
      } else {
        LOG(INFO) << "Address " << addr.ToString() << " for "
                  << host_port.ToString()
                  << " duplicates an earlier resolved entry.";
      }
    }
  }
  return Status::OK();
}

Status GetHostname(string* hostname) {
  TRACE_EVENT0("net", "GetHostname");
  char name[HOST_NAME_MAX];
  int ret = gethostname(name, HOST_NAME_MAX);
  if (ret != 0) {
    return Status::NetworkError(
        "Unable to determine local hostname", ErrnoToString(errno), errno);
  }
  *hostname = name;
  return Status::OK();
}

Status GetLocalNetworks(std::vector<Network>* net) {
  struct ifaddrs* ifap = nullptr;

  int ret = getifaddrs(&ifap);
  SCOPED_CLEANUP({
    if (ifap)
      freeifaddrs(ifap);
  });

  if (ret != 0) {
    return Status::NetworkError(
        "Unable to determine local network addresses",
        ErrnoToString(errno),
        errno);
  }

  net->clear();
  for (struct ifaddrs* ifa = ifap; ifa; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == nullptr || ifa->ifa_netmask == nullptr)
      continue;

    if (ifa->ifa_addr->sa_family == AF_INET6) {
      Sockaddr addr(*reinterpret_cast<struct sockaddr_in6*>(ifa->ifa_addr));
      Sockaddr netmask(
          *reinterpret_cast<struct sockaddr_in6*>(ifa->ifa_netmask));
      Network network(
          NetworkByteOrder::Load128(addr.addr().sin6_addr.s6_addr),
          NetworkByteOrder::Load128(netmask.addr().sin6_addr.s6_addr));
      net->push_back(network);
    }
  }

  return Status::OK();
}

Status GetFQDN(string* hostname) {
  TRACE_EVENT0("net", "GetFQDN");
  // Start with the non-qualified hostname
  RETURN_NOT_OK(GetHostname(hostname));

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_CANONNAME;
  AddrInfo result;
  const string op_description =
      Substitute("look up canonical hostname for localhost '$0'", *hostname);
  LOG_SLOW_EXECUTION(WARNING, 200, op_description) {
    TRACE_EVENT0("net", "getaddrinfo");
    RETURN_NOT_OK(GetAddrInfo(*hostname, hints, op_description, &result));
  }

  *hostname = result->ai_canonname;
  return Status::OK();
}

Status SockaddrFromHostPort(const HostPort& host_port, Sockaddr* addr) {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(host_port.ResolveAddresses(&addrs));
  if (addrs.empty()) {
    return Status::NetworkError(
        "Unable to resolve address", host_port.ToString());
  }
  *addr = addrs[0];
  if (addrs.size() > 1) {
    VLOG(1) << "Hostname " << host_port.host()
            << " resolved to more than one address. "
            << "Using address: " << addr->ToString();
  }
  return Status::OK();
}

Status HostPortFromSockaddrReplaceWildcard(const Sockaddr& addr, HostPort* hp) {
  string host;
  if (addr.IsWildcard()) {
    RETURN_NOT_OK(GetFQDN(&host));
  } else {
    host = addr.host();
  }
  hp->set_host(host);
  hp->set_port(addr.port());
  return Status::OK();
}

void TryRunLsof(const Sockaddr& addr, vector<string>* log) {
#if defined(__APPLE__)
  string cmd = strings::Substitute(
      "lsof -n -i 'TCP:$0' -sTCP:LISTEN ; "
      "for pid in $$(lsof -F p -n -i 'TCP:$0' -sTCP:LISTEN | cut -f 2 -dp) ; do"
      "  pstree $$pid || ps h -p $$pid;"
      "done",
      addr.port());
#else
  // Little inline bash script prints the full ancestry of any pid listening
  // on the same port as 'addr'. We could use 'pstree -s', but that option
  // doesn't exist on el6.
  string cmd = strings::Substitute(
      "export PATH=$$PATH:/usr/sbin ; "
      "lsof -n -i 'TCP:$0' -sTCP:LISTEN ; "
      "for pid in $$(lsof -F p -n -i 'TCP:$0' -sTCP:LISTEN | grep p | cut -f 2 -dp) ; do"
      "  while [ $$pid -gt 1 ] ; do"
      "    ps h -fp $$pid ;"
      "    stat=($$(</proc/$$pid/stat)) ;"
      "    pid=$${stat[3]} ;"
      "  done ; "
      "done",
      addr.port());
#endif // defined(__APPLE__)
  LOG_STRING(WARNING, log)
      << "Trying to use lsof to find any processes listening on "
      << addr.ToString();
  LOG_STRING(INFO, log) << "$ " << cmd;
  vector<string> argv = {"bash", "-c", cmd};
  string results;
  Status s = Subprocess::Call(argv, "", &results);
  if (PREDICT_FALSE(!s.ok())) {
    LOG_STRING(WARNING, log) << s.ToString();
  }
  LOG_STRING(WARNING, log) << results;
}

} // namespace kudu
