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

#include "kudu/util/net/socket.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cinttypes>
#include <cstring>
#include <limits>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/slice.h"

DEFINE_string(local_ip_for_outbound_sockets, "",
              "IP to bind to when making outgoing socket connections. "
              "This must be an IP address of the form A.B.C.D, not a hostname. "
              "Advanced parameter, subject to change.");
TAG_FLAG(local_ip_for_outbound_sockets, experimental);

DEFINE_bool(socket_inject_short_recvs, false,
            "Inject short recv() responses which return less data than "
            "requested");
TAG_FLAG(socket_inject_short_recvs, hidden);
TAG_FLAG(socket_inject_short_recvs, unsafe);

using std::string;
using strings::Substitute;

namespace kudu {

//todo:: currently recording stuff is baked into this class
// it should be factored out into a RecordedSocket class
// and all places where socket needs to be recorded should be modified to use that class
Socket::Socket()
  : fd_(-1), trace_(nullptr) {
}

Socket::Socket(int fd)
  : fd_(fd), trace_(nullptr) {
}

Socket::Socket(ReleasedSocket releasedSock) : fd_(releasedSock.fd), trace_(releasedSock.trace) {
}

Socket::Socket(Socket&& other) noexcept
  : Socket(other.Release()) {
}

void Socket::Reset(int fd) {
  ignore_result(Close());
  fd_ = fd;
}

ReleasedSocket Socket::Release() {
  LOG(INFO) << "SockInfo: Socket::Release() called, fd_ = " << fd_;
  int fd = fd_;
  airreplay::Trace *trace = trace_;
  fd_ = -1;
  trace_ = nullptr;
  return {fd, trace};
}

Socket::~Socket() {
  ignore_result(Close());
}

Status Socket::Close() {
  if (fd_ < 0) {
    return Status::OK();
  }
  int fd = fd_;
  int ret;
  if (trace_) {
    // this will be cleaner when I have a separate RecordedSocket class
    delete trace_;
    trace_ = nullptr;
  }
  RETRY_ON_EINTR(ret, ::close(fd));
  if (ret < 0) {
    int err = errno;
    return Status::NetworkError("close error", ErrnoToString(err), err);
  }
  fd_ = -1;
  return Status::OK();
}

Status Socket::Shutdown(bool shut_read, bool shut_write) {
  DCHECK_GE(fd_, 0);
  int flags = 0;
  if (shut_read && shut_write) {
    flags |= SHUT_RDWR;
  } else if (shut_read) {
    flags |= SHUT_RD;
  } else if (shut_write) {
    flags |= SHUT_WR;
  }
  if (::shutdown(fd_, flags) < 0) {
    int err = errno;
    return Status::NetworkError("shutdown error", ErrnoToString(err), err);
  }
  return Status::OK();
}

int Socket::GetFd() const {
  return fd_;
}

bool Socket::IsTemporarySocketError(int err) {
  return ((err == EAGAIN) || (err == EWOULDBLOCK) || (err == EINTR));
}

#if defined(__linux__)

Status Socket::Init(int family, int flags) {
  int nonblocking_flag = (flags & FLAG_NONBLOCKING) ? SOCK_NONBLOCK : 0;
  Reset(::socket(family, SOCK_STREAM | SOCK_CLOEXEC | nonblocking_flag, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError("error opening socket", ErrnoToString(err), err);
  }

  return Status::OK();
}

#else

Status Socket::Init(int family, int flags) {
  Reset(::socket(family, SOCK_STREAM, 0));
  if (fd_ < 0) {
    int err = errno;
    return Status::NetworkError("error opening socket", ErrnoToString(err), err);
  }
  RETURN_NOT_OK(SetNonBlocking(flags & FLAG_NONBLOCKING));
  RETURN_NOT_OK(SetCloseOnExec());

  // Disable SIGPIPE.
  int set = 1;
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_NOSIGPIPE, set),
                        "failed to set SO_NOSIGPIPE");
  return Status::OK();
}

#endif // defined(__linux__)

Status Socket::SetNoDelay(bool enabled) {
  int flag = enabled ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_NODELAY, flag),
                        "failed to set TCP_NODELAY");
  return Status::OK();
}

Status Socket::SetTcpCork(bool enabled) {
#if defined(__linux__)
  int flag = enabled ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_CORK, flag),
                        "failed to set TCP_CORK");
#endif // defined(__linux__)
  // TODO(unknown): Use TCP_NOPUSH for OSX if perf becomes an issue.
  return Status::OK();
}

Status Socket::SetNonBlocking(bool enabled) {
  int curflags = ::fcntl(fd_, F_GETFL, 0);
  if (curflags == -1) {
    int err = errno;
    return Status::NetworkError(
        StringPrintf("Failed to get file status flags on fd %d", fd_),
        ErrnoToString(err), err);
  }
  int newflags = (enabled) ? (curflags | O_NONBLOCK) : (curflags & ~O_NONBLOCK);
  if (::fcntl(fd_, F_SETFL, newflags) == -1) {
    int err = errno;
    if (enabled) {
      return Status::NetworkError(
          StringPrintf("Failed to set O_NONBLOCK on fd %d", fd_),
          ErrnoToString(err), err);
    } else {
      return Status::NetworkError(
          StringPrintf("Failed to clear O_NONBLOCK on fd %d", fd_),
          ErrnoToString(err), err);
    }
  }
  return Status::OK();
}

Status Socket::IsNonBlocking(bool* is_nonblock) const {
  int curflags = ::fcntl(fd_, F_GETFL, 0);
  if (curflags == -1) {
    int err = errno;
    return Status::NetworkError(
        StringPrintf("Failed to get file status flags on fd %d", fd_),
        ErrnoToString(err), err);
  }
  *is_nonblock = ((curflags & O_NONBLOCK) != 0);
  return Status::OK();
}

Status Socket::SetCloseOnExec() {
  int curflags = fcntl(fd_, F_GETFD, 0);
  if (curflags == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError("fcntl(F_GETFD) error", ErrnoToString(err), err);
  }
  if (fcntl(fd_, F_SETFD, curflags | FD_CLOEXEC) == -1) {
    int err = errno;
    Reset(-1);
    return Status::NetworkError("fcntl(F_SETFD) error", ErrnoToString(err), err);
  }
  return Status::OK();
}

Status Socket::SetSendTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_SNDTIMEO, "SO_SNDTIMEO", timeout);
}

Status Socket::SetRecvTimeout(const MonoDelta& timeout) {
  return SetTimeout(SO_RCVTIMEO, "SO_RCVTIMEO", timeout);
}

Status Socket::SetReuseAddr(bool flag) {
  int int_flag = flag ? 1 : 0;
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_REUSEADDR, int_flag),
                        "failed to set SO_REUSEADDR");
  return Status::OK();
}

Status Socket::SetReusePort(bool flag) {
  #ifdef SO_REUSEPORT
    int int_flag = flag ? 1 : 0;
    RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_REUSEPORT, int_flag),
                          "failed to set SO_REUSEPORT");
    return Status::OK();
  #else
    return Status::NotSupported("failed to set SO_REUSEPORT: protocol not available");
  #endif
}

Status Socket::BindAndListen(const Sockaddr &sockaddr,
                             int listen_queue_size) {
  RETURN_NOT_OK(SetReuseAddr(true));
  RETURN_NOT_OK(Bind(sockaddr));
  RETURN_NOT_OK(Listen(listen_queue_size));
  return Status::OK();
}

Status Socket::Listen(int listen_queue_size) {
  if (listen(fd_, listen_queue_size)) {
    int err = errno;
    return Status::NetworkError("listen() error", ErrnoToString(err));
  }
  // no need to record listen()-ed sockets
  return Status::OK();
}

Status Socket::GetSocketAddress(Sockaddr *cur_addr) const {
  struct sockaddr_storage ss;
  socklen_t len = sizeof(ss);
  DCHECK_GE(fd_, 0);
  if (::getsockname(fd_, reinterpret_cast<struct sockaddr*>(&ss), &len) == -1) {
    int err = errno;
    return Status::NetworkError("getsockname error", ErrnoToString(err), err);
  }
  *cur_addr = Sockaddr(reinterpret_cast<struct sockaddr&>(ss), len);
  return Status::OK();
}

Status Socket::GetPeerAddress(Sockaddr *cur_addr) const {
  struct sockaddr_storage addr;
  socklen_t len = sizeof(addr);
  DCHECK_GE(fd_, 0);
  if (::getpeername(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) == -1) {
    int err = errno;
    return Status::NetworkError("getpeername error", ErrnoToString(err), err);
  }
  *cur_addr = Sockaddr(reinterpret_cast<const sockaddr&>(addr), len);
  return Status::OK();
}

bool Socket::IsLoopbackConnection() const {
  Sockaddr local, remote;
  if (!GetSocketAddress(&local).ok()) return false;
  if (!GetPeerAddress(&remote).ok()) return false;
  // Check if remote address is in 127.0.0.0/8 subnet.
  if (remote.IsAnyLocalAddress()) {
    return true;
  }
  // Compare local and remote addresses without comparing ports.
  local.set_port(0);
  remote.set_port(0);
  return local == remote;
}

Status Socket::Bind(const Sockaddr& bind_addr) {
  DCHECK_GE(fd_, 0);
  LOG(INFO) << "SockInfo: Binding socket to " << bind_addr.ToString();
  if (PREDICT_FALSE(::bind(fd_, bind_addr.addr(), bind_addr.addrlen()))) {
    int err = errno;
    Status s = Status::NetworkError(
        strings::Substitute("error binding socket to $0: $1",
                            bind_addr.ToString(), ErrnoToString(err)),
        Slice(), err);

    if (s.IsNetworkError() && bind_addr.is_ip() &&
        s.posix_code() == EADDRINUSE && bind_addr.port() != 0) {
      TryRunLsof(bind_addr);
    }
    return s;
  }

  return Status::OK();
}

Status Socket::Accept(Socket *new_conn, Sockaddr *remote, int flags) {
  TRACE_EVENT0("net", "Socket::Accept");
  struct sockaddr_storage addr;
  socklen_t olen = sizeof(addr);
  DCHECK_GE(fd_, 0);
#if defined(__linux__)
  int accept_flags = SOCK_CLOEXEC;
  if (flags & FLAG_NONBLOCKING) {
    accept_flags |= SOCK_NONBLOCK;
  }
  int fd = -1;
  RETRY_ON_EINTR(fd, accept4(fd_, (struct sockaddr*)&addr,
                             &olen, accept_flags));
  if (fd < 0) {
    int err = errno;
    return Status::NetworkError("accept4(2) error", ErrnoToString(err), err);
  }
  new_conn->Reset(fd);

#else
  int fd = -1;
  RETRY_ON_EINTR(fd, accept(fd_, (struct sockaddr*)&addr, &olen));
  if (fd < 0) {
    int err = errno;
    return Status::NetworkError("accept(2) error", ErrnoToString(err), err);
  }
  new_conn->Reset(fd);
  RETURN_NOT_OK(new_conn->SetNonBlocking(flags & FLAG_NONBLOCKING));
  RETURN_NOT_OK(new_conn->SetCloseOnExec());
#endif // defined(__linux__)

  *remote = Sockaddr(reinterpret_cast<const sockaddr&>(addr), olen);
  TRACE_EVENT_INSTANT1("net", "Accepted", TRACE_EVENT_SCOPE_THREAD,
                       "remote", remote->ToString());
  Sockaddr local;
  DCHECK(GetSocketAddress(&local).ok());
  LOG(INFO) << "SockInfo: Accepted to " << local.ToString() << " from " << remote->ToString();
  std::string filename = "socket_rec_accept_to_" + local.ToString() + "_as_" + remote->ToString() ;
  new_conn->trace_ = new airreplay::Trace(filename, airreplay::airr->isReplay() ? airreplay::Mode::kReplay : airreplay::Mode::kRecord);
  return Status::OK();
}

Status Socket::BindForOutgoingConnection() {
  Sockaddr bind_host;
  Status s = bind_host.ParseString(FLAGS_local_ip_for_outbound_sockets, 0);
  CHECK(s.ok() && bind_host.port() == 0)
    << "Invalid local IP set for 'local_ip_for_outbound_sockets': '"
    << FLAGS_local_ip_for_outbound_sockets << "': " << s.ToString();

  LOG(INFO) << "SockInfo: Binding (for outbound) socket to " << bind_host.ToString();
  RETURN_NOT_OK(Bind(bind_host));
  return Status::OK();
}

Status Socket::Connect(const Sockaddr &remote_const) {
  Sockaddr remote = remote_const;
  TRACE_EVENT1("net", "Socket::Connect",
               "remote", remote.ToString());
  if (PREDICT_FALSE(!FLAGS_local_ip_for_outbound_sockets.empty())) {
    RETURN_NOT_OK(BindForOutgoingConnection());
  }

  // in replay, reroute connections to the special mock-socket-replayer which will accept connections, 
  // verify that it received messages it expected, and will replay necessary responses
  // In cases of most connections, we could listen to the original localhost:* addresses to make this more general.
  // But, there is an edge case where the server sends a message to self. In those cases we want to intercept the connection
  // and place the mock-socket-replayer in between. This seems an easy way to achieve that. perhaps there are nicer ways.
  if (airreplay::airr->isReplay() && (remote.host() == "localhost" || remote.host() == "127.0.0.1")) {
    LOG(INFO) << "changed connection address";
    Status s = remote.ParseString("10.0.0.0", remote.port());
    DCHECK(s.ok());
  }

  DCHECK_GE(fd_, 0);
  int ret;
  RETRY_ON_EINTR(ret, ::connect(fd_, remote.addr(), remote.addrlen()));
  int err = errno;
  
  // in all these cases connection is established or in progress
  // we should create a connection recording
  if (ret >= 0 || Socket::IsTemporarySocketError(err) || err == EINPROGRESS) {
      DCHECK(airreplay::airr != nullptr) << "airreplay::airr is null";
      Sockaddr local;
      DCHECK(GetSocketAddress(&local).ok());
      LOG(INFO) << "SockInfo: connecting to " << remote.ToString();
      std::string filename = "socket_rec_connect_from_" + local.ToString() + "_from_" + remote.ToString();
      trace_ = new airreplay::Trace(filename, airreplay::airr->isReplay() ? airreplay::Mode::kReplay : airreplay::Mode::kRecord, false);
  }

  if (ret < 0) {
    return Status::NetworkError("connect(2) error", ErrnoToString(err), err);
  }
  return Status::OK();
}

Status Socket::GetSockError() const {
  int val = 0, ret;
  socklen_t val_len = sizeof(val);
  DCHECK_GE(fd_, 0);
  ret = ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &val, &val_len);
  if (ret) {
    int err = errno;
    return Status::NetworkError("getsockopt(SO_ERROR) failed", ErrnoToString(err), err);
  }
  if (val != 0) {
    return Status::NetworkError(ErrnoToString(val), Slice(), val);
  }
  return Status::OK();
}

Status Socket::Write(const uint8_t *buf, int32_t amt, int32_t *nwritten) {
  if (amt <= 0) {
    return Status::NetworkError(
              StringPrintf("invalid send of %" PRId32 " bytes",
                           amt), Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);
  int res;
  RETRY_ON_EINTR(res, ::send(fd_, buf, amt, MSG_NOSIGNAL));
  if (res < 0) {
    int err = errno;
    return Status::NetworkError("write error", ErrnoToString(err), err);
  }
  DCHECK(trace_) << "Socket::Write() called without trace_";
  std::string payload((const char *)buf, res);
  if (trace_->isReplay()) {
    // if (!trace_->HasNext()) {
    // LOG(WARNING) << "trace empty but socket was written to in replay";
    // return Status::EndOfFile("Reached to the end of replayed socket trace");
    // }

    // int pos = 0;
    // airreplay::OpequeEntry next = trace_->PeekNext(&pos);
    // if (next.bytes_message() != payload) {
    //   LOG(WARNING) << "recorded and replayed socket messages differ@" << std::to_string(pos) << "recorded: " << next.bytes_message() <<std::endl <<  "replayed: " << payload;
    // }
  } else {
    trace_->Record(payload, "Socket Write");
  }

  *nwritten = res;
  return Status::OK();
}

Status Socket::Writev(const struct ::iovec *iov, int iov_len,
                      int64_t *nwritten) {
  if (PREDICT_FALSE(iov_len <= 0)) {
    return Status::NetworkError(
                StringPrintf("writev: invalid io vector length of %d",
                             iov_len),
                Slice(), EINVAL);
  }
  DCHECK_GE(fd_, 0);

  struct msghdr msg;
  memset(&msg, 0, sizeof(struct msghdr));
  msg.msg_iov = const_cast<iovec *>(iov);
  msg.msg_iovlen = iov_len;
  ssize_t res;
  RETRY_ON_EINTR(res, ::sendmsg(fd_, &msg, MSG_NOSIGNAL));
  if (PREDICT_FALSE(res < 0)) {
    int err = errno;
    return Status::NetworkError("sendmsg error", ErrnoToString(err), err);
  }
  DCHECK(trace_) << "Socket::Writev() called without trace_";
  for (int i = 0; i < iov_len; i++) {

    std::string payload(static_cast<char*>(iov[i].iov_base), iov[i].iov_len);
    if (trace_->isReplay()) {
      // if (!trace_->HasNext()) {
      // LOG(WARNING) << "trace empty but socket was written to in replay";
      // return Status::EndOfFile("Reached to the end of replayed socket trace");
      // }

      // int pos = 0;
      // airreplay::OpequeEntry next = trace_->PeekNext(&pos);
      // if (next.bytes_message() != payload) {
      //   LOG(WARNING) << "recorded and replayed socket messages differ@" << std::to_string(pos) << "recorded: " << next.bytes_message() <<std::endl <<  "replayed: " << payload;
      // }
    } else {
      trace_->Record(payload, "Socket writev of nvectors=" + std::to_string(i) + "/" + std::to_string(iov_len));
    } 
  }
  *nwritten = res;
  return Status::OK();
}

// Mostly follows writen() from Stevens (2004) or Kerrisk (2010).
Status Socket::BlockingWrite(const uint8_t *buf, size_t buflen, size_t *nwritten,
    const MonoTime& deadline) {
  DCHECK_LE(buflen, std::numeric_limits<int32_t>::max()) << "Writes > INT32_MAX not supported";
  DCHECK(nwritten);

  size_t tot_written = 0;
  while (tot_written < buflen) {
    int32_t inc_num_written = 0;
    int32_t num_to_write = buflen - tot_written;
    MonoDelta timeout = deadline - MonoTime::Now();
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut(Substitute("sent $0 of $1 requested bytes",
                                         tot_written, buflen));
    }
    RETURN_NOT_OK(SetSendTimeout(timeout));
    Status s = Write(buf, num_to_write, &inc_num_written);
    tot_written += inc_num_written;
    buf += inc_num_written;
    *nwritten = tot_written;

    if (PREDICT_FALSE(!s.ok())) {
      // Continue silently when the syscall is interrupted.
      if (s.posix_code() == EINTR) {
        continue;
      }
      if (s.posix_code() == EAGAIN) {
        return Status::TimedOut(Substitute("sent $0 of $1 requested bytes",
                                           tot_written, buflen));
      }
      return s.CloneAndPrepend("BlockingWrite error");
    }
    if (PREDICT_FALSE(inc_num_written == 0)) {
      // Shouldn't happen on Linux with a blocking socket. Maybe other Unices.
      break;
    }
  }

  if (tot_written < buflen) {
    return Status::IOError("Wrote zero bytes on a BlockingWrite() call",
        StringPrintf("Transferred %zu of %zu bytes", tot_written, buflen));
  }
  return Status::OK();
}

Status Socket::Recv(uint8_t *buf, int32_t amt, int32_t *nread) {
  if (amt <= 0) {
    return Status::NetworkError(
          StringPrintf("invalid recv of %d bytes", amt), Slice(), EINVAL);
  }

  // The recv() call can return fewer than the requested number of bytes.
  // Especially when 'amt' is small, this is very unlikely to happen in
  // the context of unit tests. So, we provide an injection hook which
  // simulates the same behavior.
  if (PREDICT_FALSE(FLAGS_socket_inject_short_recvs && amt > 1)) {
    Random r(GetRandomSeed32());
    amt = 1 + r.Uniform(amt - 1);
  }

  DCHECK_GE(fd_, 0);
  int res;
  RETRY_ON_EINTR(res, recv(fd_, buf, amt, 0));
  if (res <= 0) {
    Sockaddr remote;
    Status get_addr_status = GetPeerAddress(&remote);
    string remote_str = get_addr_status.ok() ? remote.ToString() : "unknown peer";
    if (res == 0) {
      string error_message = Substitute("recv got EOF from $0", remote_str);
      return Status::NetworkError(error_message, Slice(), ESHUTDOWN);
    }
    int err = errno;
    string error_message = Substitute("recv error from $0", remote_str);
    return Status::NetworkError(error_message, ErrnoToString(err), err);
  }
  *nread = res;
  DCHECK(trace_) << "Socket::Recv() called without trace_";
  std::string payload((const char *)buf, res);
  
  if (trace_->isReplay()) {
    // if (!trace_->HasNext()) {
    //   LOG(WARNING) << "trace empty but socket was read from in replay";
    //   return Status::EndOfFile("Reached to the end of replayed socket trace");
    // }
    // int pos = 0;
    // airreplay::OpequeEntry next = trace_->PeekNext(&pos);
    // if (next.bytes_message() != payload) {
    //   LOG(WARNING) << "recorded and replayed socket messages differ@" << std::to_string(pos) << "recorded: " << next.bytes_message() <<std::endl <<  "replayed: " << payload;
    // }
  } else {
  trace_->Record(payload, "Socket Read");
  } 

  return Status::OK();
}

// Mostly follows readn() from Stevens (2004) or Kerrisk (2010).
// One place where we deviate: we consider EOF a failure if < amt bytes are read.
Status Socket::BlockingRecv(uint8_t *buf, size_t amt, size_t *nread, const MonoTime& deadline) {
  DCHECK_LE(amt, std::numeric_limits<int32_t>::max()) << "Reads > INT32_MAX not supported";
  DCHECK(nread);
  size_t tot_read = 0;
  while (tot_read < amt) {
    int32_t inc_num_read = 0;
    int32_t num_to_read = amt - tot_read;
    MonoDelta timeout = deadline - MonoTime::Now();
    if (PREDICT_FALSE(timeout.ToNanoseconds() <= 0)) {
      return Status::TimedOut(Substitute("received $0 of $1 requested bytes",
                                         tot_read, amt));
    }
    RETURN_NOT_OK(SetRecvTimeout(timeout));
    Status s = Recv(buf, num_to_read, &inc_num_read);
    tot_read += inc_num_read;
    buf += inc_num_read;
    *nread = tot_read;

    if (PREDICT_FALSE(!s.ok())) {
      // Continue silently when the syscall is interrupted.
      if (s.posix_code() == EINTR) {
        continue;
      }
      if (s.posix_code() == EAGAIN) {
        return Status::TimedOut(Substitute("received $0 of $1 requested bytes",
                                           tot_read, amt));
      }
      return s.CloneAndPrepend("BlockingRecv error");
    }
    if (PREDICT_FALSE(inc_num_read == 0)) {
      // EOF.
      break;
    }
  }

  if (PREDICT_FALSE(tot_read < amt)) {
    return Status::IOError("Read zero bytes on a blocking Recv() call",
        StringPrintf("Transferred %zu of %zu bytes", tot_read, amt));
  }
  return Status::OK();
}

Status Socket::SetTimeout(int opt, const char* optname, const MonoDelta& timeout) {
  if (PREDICT_FALSE(timeout.ToNanoseconds() < 0)) {
    return Status::InvalidArgument("Timeout specified as negative to SetTimeout",
                                   timeout.ToString());
  }
  struct timeval tv;
  timeout.ToTimeVal(&tv);
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, opt, tv),
                        Substitute("failed to set socket option $0 to $1",
                                   optname, timeout.ToString()));
  return Status::OK();
}

Status Socket::SetTcpKeepAlive(int idle_time_s, int retry_time_s, int num_retries) {
#if defined(__linux__)
  static const char* const err_string = "failed to set socket option $0 to $1";
  DCHECK_GT(idle_time_s, 0);
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPIDLE, idle_time_s),
      Substitute(err_string, "TCP_KEEPIDLE", idle_time_s));
  DCHECK_GT(retry_time_s, 0);
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPINTVL, retry_time_s),
      Substitute(err_string, "TCP_KEEPINTVL", retry_time_s));
  DCHECK_GT(num_retries, 0);
  RETURN_NOT_OK_PREPEND(SetSockOpt(IPPROTO_TCP, TCP_KEEPCNT, num_retries),
      Substitute(err_string, "TCP_KEEPCNT", num_retries));
  RETURN_NOT_OK_PREPEND(SetSockOpt(SOL_SOCKET, SO_KEEPALIVE, 1),
      "failed to enable TCP KeepAlive socket option");
#endif
  return Status::OK();
}

template<typename T>
Status Socket::SetSockOpt(int level, int option, const T& value) {
  if (::setsockopt(fd_, level, option, &value, sizeof(T)) == -1) {
    int err = errno;
    return Status::NetworkError(ErrnoToString(err), Slice(), err);
  }
  return Status::OK();
}

} // namespace kudu
