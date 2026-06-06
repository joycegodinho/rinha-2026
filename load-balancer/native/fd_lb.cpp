#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <sys/un.h>
#include <thread>
#include <vector>
#include <time.h>
#include <unistd.h>

namespace {

#if ENABLE_BRANCH_HINTS && (defined(__GNUC__) || defined(__clang__))
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#ifndef SO_BUSY_POLL
#define SO_BUSY_POLL 46
#endif
#ifndef SO_PREFER_BUSY_POLL
#define SO_PREFER_BUSY_POLL 69
#endif
#ifndef SO_BUSY_POLL_BUDGET
#define SO_BUSY_POLL_BUDGET 70
#endif
#ifndef TCP_FASTOPEN
#define TCP_FASTOPEN 23
#endif
#ifndef TCP_QUICKACK
#define TCP_QUICKACK 12
#endif

constexpr int kBacklog = 4096;
constexpr int kMaxFdBatch = 16;
constexpr size_t kUnixPathMax = sizeof(sockaddr_un{}.sun_path);
int g_defer_accept_seconds = 1;
int g_initial_connect_retries = 3000;
int g_reconnect_retries = 3;
int g_connect_sleep_ms = 10;
int g_tcp_rcvbuf = 0;
int g_tcp_sndbuf = 0;
int g_control_rcvbuf = 0;
int g_control_sndbuf = 0;
bool g_set_accept_nodelay = false;
bool g_set_accept_quickack = false;
bool g_blocking_accept = false;
int g_listen_port = 9999;
int g_self_warm_requests = 0;
int g_self_warm_delay_ms = 20;
int g_socket_busy_poll_us = 0;
int g_socket_busy_poll_budget = 0;
bool g_socket_prefer_busy_poll = false;
int g_timer_slack_ns = 0;
int g_fd_batch = 1;
int g_tcp_fastopen = 0;
bool g_control_dontwait = false;
bool g_control_seqpacket = false;
bool g_lb_profile = false;
bool g_pin_workers = false;
int g_control_channels_per_target = 1;

constexpr int kLbProfileBinCount = 9;
constexpr uint64_t kLbProfilePrintEvery = 32;
constexpr uint64_t kLbProfileLimits[kLbProfileBinCount - 1] = {
    1000ULL, 2500ULL, 5000ULL, 10000ULL, 25000ULL, 50000ULL, 100000ULL, 250000ULL};

struct LbProfileCounters {
  std::atomic<uint64_t> accepts{0};
  std::atomic<uint64_t> accept_ns{0};
  std::atomic<uint64_t> accept_max_ns{0};
  std::atomic<uint64_t> accept_bins[kLbProfileBinCount]{};
  std::atomic<uint64_t> accept_eagain{0};
  std::atomic<uint64_t> sendmsg_calls{0};
  std::atomic<uint64_t> sendmsg_ns{0};
  std::atomic<uint64_t> sendmsg_max_ns{0};
  std::atomic<uint64_t> sendmsg_bins[kLbProfileBinCount]{};
  std::atomic<uint64_t> poll_waits{0};
  std::atomic<uint64_t> poll_wait_ns{0};
  std::atomic<uint64_t> poll_wait_max_ns{0};
  std::atomic<uint64_t> poll_wait_bins[kLbProfileBinCount]{};
  std::atomic<uint64_t> last_print_accepts{0};
};

LbProfileCounters g_lb_prof;

struct Backend {
  const char* path = nullptr;
  size_t path_len = 0;
  int ctrl_fd = -1;
  char send_byte = 0;
  iovec send_iov{};
  char send_control[CMSG_SPACE(sizeof(int))]{};
  msghdr send_msg{};
  cmsghdr* send_cmsg = nullptr;
};

struct FdBatch {
  int fds[kMaxFdBatch]{};
  int count = 0;
};

struct WorkerConfig {
  int listen_fd = -1;
  Backend target1{};
  Backend target2{};
  std::atomic<uint64_t>* rr = nullptr;
  int worker_id = 0;
  bool local_rr = false;
};

void close_fd(int fd) {
  if (LIKELY(fd >= 0)) close(fd);
}

uint64_t now_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

void atomic_max(std::atomic<uint64_t>& target, uint64_t value) {
  uint64_t current = target.load(std::memory_order_relaxed);
  while (value > current &&
         !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
  }
}

void record_lb_ns(uint64_t ns, std::atomic<uint64_t> (&bins)[kLbProfileBinCount],
                  std::atomic<uint64_t>& total, std::atomic<uint64_t>& max_value) {
  total.fetch_add(ns, std::memory_order_relaxed);
  atomic_max(max_value, ns);
  int idx = kLbProfileBinCount - 1;
  for (int i = 0; i < kLbProfileBinCount - 1; ++i) {
    if (ns < kLbProfileLimits[i]) {
      idx = i;
      break;
    }
  }
  bins[idx].fetch_add(1, std::memory_order_relaxed);
}

void print_lb_bins(const char* label, const std::atomic<uint64_t> (&bins)[kLbProfileBinCount]) {
  std::fprintf(stderr,
               "%s <1us=%llu <2.5us=%llu <5us=%llu <10us=%llu <25us=%llu "
               "<50us=%llu <100us=%llu <250us=%llu >=250us=%llu\n",
               label,
               static_cast<unsigned long long>(bins[0].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[1].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[2].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[3].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[4].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[5].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[6].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[7].load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(bins[8].load(std::memory_order_relaxed)));
}

void print_lb_profile() {
  if (!g_lb_profile) return;
  const uint64_t accepts = g_lb_prof.accepts.load(std::memory_order_relaxed);
  const uint64_t sendmsg_calls = g_lb_prof.sendmsg_calls.load(std::memory_order_relaxed);
  const uint64_t poll_waits = g_lb_prof.poll_waits.load(std::memory_order_relaxed);
  const uint64_t accept_ns = g_lb_prof.accept_ns.load(std::memory_order_relaxed);
  const uint64_t sendmsg_ns = g_lb_prof.sendmsg_ns.load(std::memory_order_relaxed);
  const uint64_t poll_wait_ns = g_lb_prof.poll_wait_ns.load(std::memory_order_relaxed);
  std::fprintf(stderr,
               "[fd-lb-profile] accepts=%llu accept_eagain=%llu avg_accept_ns=%llu "
               "max_accept_ns=%llu sendmsg_calls=%llu avg_sendmsg_ns=%llu "
               "max_sendmsg_ns=%llu poll_waits=%llu avg_poll_wait_ns=%llu "
               "max_poll_wait_ns=%llu\n",
               static_cast<unsigned long long>(accepts),
               static_cast<unsigned long long>(g_lb_prof.accept_eagain.load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(accepts ? accept_ns / accepts : 0),
               static_cast<unsigned long long>(g_lb_prof.accept_max_ns.load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(sendmsg_calls),
               static_cast<unsigned long long>(sendmsg_calls ? sendmsg_ns / sendmsg_calls : 0),
               static_cast<unsigned long long>(g_lb_prof.sendmsg_max_ns.load(std::memory_order_relaxed)),
               static_cast<unsigned long long>(poll_waits),
               static_cast<unsigned long long>(poll_waits ? poll_wait_ns / poll_waits : 0),
               static_cast<unsigned long long>(g_lb_prof.poll_wait_max_ns.load(std::memory_order_relaxed)));
  print_lb_bins("[fd-lb-profile] accept_bins", g_lb_prof.accept_bins);
  print_lb_bins("[fd-lb-profile] sendmsg_bins", g_lb_prof.sendmsg_bins);
  print_lb_bins("[fd-lb-profile] poll_wait_bins", g_lb_prof.poll_wait_bins);
}

void maybe_print_lb_profile() {
  if (!g_lb_profile) return;
  const uint64_t accepts = g_lb_prof.accepts.load(std::memory_order_relaxed);
  uint64_t last = g_lb_prof.last_print_accepts.load(std::memory_order_relaxed);
  if (accepts < last + kLbProfilePrintEvery) return;
  if (g_lb_prof.last_print_accepts.compare_exchange_strong(
          last, accepts, std::memory_order_relaxed)) {
    print_lb_profile();
  }
}

int set_blocking(int fd, bool blocking) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (UNLIKELY(flags < 0)) return -1;
  if (blocking) {
    flags &= ~O_NONBLOCK;
  } else {
    flags |= O_NONBLOCK;
  }
  return fcntl(fd, F_SETFL, flags);
}

int env_int(const char* name, int fallback, int min_value, int max_value) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (UNLIKELY(end == value || parsed < min_value || parsed > max_value)) return fallback;
  return static_cast<int>(parsed);
}

bool env_bool(const char* name, bool fallback) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  return value[0] == '1' || value[0] == 'y' || value[0] == 'Y' ||
         value[0] == 't' || value[0] == 'T';
}

void maybe_pin_worker_cpu(int worker_id) {
  if (!g_pin_workers) return;

  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0) {
    std::perror("fd lb sched_getaffinity");
    return;
  }

  int allowed_count = 0;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &allowed)) ++allowed_count;
  }
  if (allowed_count <= 0) return;

  int wanted = worker_id % allowed_count;
  int selected = -1;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (!CPU_ISSET(cpu, &allowed)) continue;
    if (wanted == 0) {
      selected = cpu;
      break;
    }
    --wanted;
  }
  if (selected < 0) return;

  cpu_set_t target;
  CPU_ZERO(&target);
  CPU_SET(selected, &target);
  if (sched_setaffinity(0, sizeof(target), &target) != 0) {
    std::perror("fd lb sched_setaffinity");
    return;
  }
  std::fprintf(stderr, "fd lb worker %d pinned to cpu %d\n", worker_id, selected);
}

void set_socket_buffers(int fd, int rcvbuf, int sndbuf) {
  if (rcvbuf > 0) {
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  }
  if (sndbuf > 0) {
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
  }
}

void set_socket_busy_poll(int fd) {
  if (g_socket_prefer_busy_poll) {
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_PREFER_BUSY_POLL, &one, sizeof(one));
  }
  if (g_socket_busy_poll_us > 0) {
    int usecs = g_socket_busy_poll_us;
    setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &usecs, sizeof(usecs));
  }
  if (g_socket_busy_poll_budget > 0) {
    int budget = g_socket_busy_poll_budget;
    setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL_BUDGET, &budget, sizeof(budget));
  }
}

int create_tcp_listener(const char* addr_env, bool reuse_port) {
  const char* addr = (addr_env && addr_env[0] != '\0') ? addr_env : "0.0.0.0:9999";
  const char* colon = std::strrchr(addr, ':');
  if (UNLIKELY(!colon)) return -1;

  char host[INET_ADDRSTRLEN]{};
  size_t host_len = static_cast<size_t>(colon - addr);
  if (host_len == 0) {
    std::memcpy(host, "0.0.0.0", sizeof("0.0.0.0"));
  } else {
    if (UNLIKELY(host_len >= sizeof(host))) return -1;
    std::memcpy(host, addr, host_len);
    host[host_len] = '\0';
  }

  char* end = nullptr;
  long parsed_port = std::strtol(colon + 1, &end, 10);
  if (UNLIKELY(end == colon + 1 || *end != '\0' || parsed_port <= 0 || parsed_port > 65535)) {
    return -1;
  }
  g_listen_port = static_cast<int>(parsed_port);

  int socket_flags = SOCK_STREAM | SOCK_CLOEXEC;
  if (!g_blocking_accept) {
    socket_flags |= SOCK_NONBLOCK;
  }
  int fd = socket(AF_INET, socket_flags, 0);
  if (UNLIKELY(fd < 0)) return -1;
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  set_socket_buffers(fd, g_tcp_rcvbuf, g_tcp_sndbuf);
  if (reuse_port) {
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
  }
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &g_defer_accept_seconds, sizeof(g_defer_accept_seconds));
  if (g_tcp_fastopen > 0) {
    setsockopt(fd, IPPROTO_TCP, TCP_FASTOPEN, &g_tcp_fastopen, sizeof(g_tcp_fastopen));
  }
  set_socket_busy_poll(fd);

  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(parsed_port));
  if (UNLIKELY(inet_pton(AF_INET, host, &sa.sin_addr) != 1)) {
    close_fd(fd);
    return -1;
  }
  if (UNLIKELY(bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0)) {
    close_fd(fd);
    return -1;
  }
  if (UNLIKELY(listen(fd, kBacklog) < 0)) {
    close_fd(fd);
    return -1;
  }
  return fd;
}

int connect_unix_socket(const Backend& backend) {
  int socket_type = g_control_seqpacket ? SOCK_SEQPACKET : SOCK_STREAM;
  int fd = socket(AF_UNIX, socket_type | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
  if (UNLIKELY(fd < 0)) return -1;
  set_socket_buffers(fd, g_control_rcvbuf, g_control_sndbuf);

  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  std::memcpy(sa.sun_path, backend.path, backend.path_len);
  sa.sun_path[backend.path_len] = '\0';

  if (UNLIKELY(connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0)) {
    if (errno != EINPROGRESS && errno != EAGAIN) {
      close_fd(fd);
      return -1;
    }

    pollfd pfd{fd, POLLOUT, 0};
    int timeout_ms = g_connect_sleep_ms > 0 ? g_connect_sleep_ms : 1;
    int poll_result;
    do {
      poll_result = poll(&pfd, 1, timeout_ms);
    } while (poll_result < 0 && errno == EINTR);
    if (poll_result <= 0 || !(pfd.revents & POLLOUT)) {
      close_fd(fd);
      return -1;
    }

    int err = 0;
    socklen_t err_len = sizeof(err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len) < 0 || err != 0) {
      close_fd(fd);
      return -1;
    }
  }

  if (UNLIKELY(set_blocking(fd, true) < 0)) {
    close_fd(fd);
    return -1;
  }
  return fd;
}

int send_passed_fds_errno(int sock, const int* fds, int count, bool dontwait);
int send_passed_fd_prebuilt_errno(Backend& backend, int fd_to_send, bool dontwait);
bool send_passed_fds(int sock, const int* fds, int count);
bool send_to_backend_batch(Backend& backend, const int* fds, int count);

bool send_passed_fd(int sock, int fd_to_send) {
  return send_passed_fds(sock, &fd_to_send, 1);
}

int send_passed_fds_errno(int sock, const int* fds, int count, bool dontwait) {
  if (UNLIKELY(count <= 0)) return 0;
  if (UNLIKELY(count > kMaxFdBatch)) count = kMaxFdBatch;
  char byte = 0;
  iovec iov{};
  iov.iov_base = &byte;
  iov.iov_len = 1;

  char control[CMSG_SPACE(sizeof(int) * kMaxFdBatch)]{};
  msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = CMSG_SPACE(static_cast<size_t>(count) * sizeof(int));

  cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(static_cast<size_t>(count) * sizeof(int));
  std::memcpy(CMSG_DATA(cmsg), fds, static_cast<size_t>(count) * sizeof(int));

  const int flags = MSG_NOSIGNAL | (dontwait ? MSG_DONTWAIT : 0);
  const uint64_t profile_t0 = g_lb_profile ? now_ns() : 0;
  for (;;) {
    ssize_t n = sendmsg(sock, &msg, flags);
    if (LIKELY(n == 1)) {
      if (g_lb_profile) {
        g_lb_prof.sendmsg_calls.fetch_add(1, std::memory_order_relaxed);
        record_lb_ns(now_ns() - profile_t0, g_lb_prof.sendmsg_bins,
                     g_lb_prof.sendmsg_ns, g_lb_prof.sendmsg_max_ns);
      }
      return 0;
    }
    if (UNLIKELY(n < 0 && errno == EINTR)) continue;
    if (g_lb_profile) {
      g_lb_prof.sendmsg_calls.fetch_add(1, std::memory_order_relaxed);
      record_lb_ns(now_ns() - profile_t0, g_lb_prof.sendmsg_bins,
                   g_lb_prof.sendmsg_ns, g_lb_prof.sendmsg_max_ns);
    }
    return n < 0 ? errno : EIO;
  }
}

bool send_passed_fds(int sock, const int* fds, int count) {
  return send_passed_fds_errno(sock, fds, count, false) == 0;
}

bool is_backpressure_errno(int err) {
  return err == EAGAIN || err == EWOULDBLOCK;
}

void init_backend_send_msg(Backend& backend) {
  backend.send_byte = 0;
  std::memset(&backend.send_iov, 0, sizeof(backend.send_iov));
  std::memset(backend.send_control, 0, sizeof(backend.send_control));
  std::memset(&backend.send_msg, 0, sizeof(backend.send_msg));
  backend.send_iov.iov_base = &backend.send_byte;
  backend.send_iov.iov_len = 1;
  backend.send_msg.msg_iov = &backend.send_iov;
  backend.send_msg.msg_iovlen = 1;
  backend.send_msg.msg_control = backend.send_control;
  backend.send_msg.msg_controllen = sizeof(backend.send_control);
  backend.send_cmsg = CMSG_FIRSTHDR(&backend.send_msg);
  if (backend.send_cmsg) {
    backend.send_cmsg->cmsg_level = SOL_SOCKET;
    backend.send_cmsg->cmsg_type = SCM_RIGHTS;
    backend.send_cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  }
}

int send_passed_fd_prebuilt_errno(Backend& backend, int fd_to_send, bool dontwait) {
  if (UNLIKELY(!backend.send_cmsg)) return EINVAL;
  backend.send_msg.msg_controllen = CMSG_SPACE(sizeof(int));
  std::memcpy(CMSG_DATA(backend.send_cmsg), &fd_to_send, sizeof(fd_to_send));

  const int flags = MSG_NOSIGNAL | (dontwait ? MSG_DONTWAIT : 0);
  const uint64_t profile_t0 = g_lb_profile ? now_ns() : 0;
  for (;;) {
    ssize_t n = sendmsg(backend.ctrl_fd, &backend.send_msg, flags);
    if (LIKELY(n == 1)) {
      if (g_lb_profile) {
        g_lb_prof.sendmsg_calls.fetch_add(1, std::memory_order_relaxed);
        record_lb_ns(now_ns() - profile_t0, g_lb_prof.sendmsg_bins,
                     g_lb_prof.sendmsg_ns, g_lb_prof.sendmsg_max_ns);
      }
      return 0;
    }
    if (UNLIKELY(n < 0 && errno == EINTR)) continue;
    if (g_lb_profile) {
      g_lb_prof.sendmsg_calls.fetch_add(1, std::memory_order_relaxed);
      record_lb_ns(now_ns() - profile_t0, g_lb_prof.sendmsg_bins,
                   g_lb_prof.sendmsg_ns, g_lb_prof.sendmsg_max_ns);
    }
    return n < 0 ? errno : EIO;
  }
}

bool connect_backend(Backend& backend, int retries) {
  for (int i = 0; i < retries; ++i) {
    backend.ctrl_fd = connect_unix_socket(backend);
    if (LIKELY(backend.ctrl_fd >= 0)) {
      init_backend_send_msg(backend);
      return true;
    }
    if (g_connect_sleep_ms > 0) {
      timespec ts{g_connect_sleep_ms / 1000, (g_connect_sleep_ms % 1000) * 1000 * 1000};
      nanosleep(&ts, nullptr);
    }
  }
  return false;
}

bool send_to_backend(Backend& backend, int cfd) {
  return send_to_backend_batch(backend, &cfd, 1);
}

bool send_to_backend_batch_mode(Backend& backend, const int* fds, int count,
                                bool dontwait, int* out_errno) {
  int err = count == 1 ? send_passed_fd_prebuilt_errno(backend, fds[0], dontwait)
                       : send_passed_fds_errno(backend.ctrl_fd, fds, count, dontwait);
  if (LIKELY(err == 0)) {
    if (out_errno) *out_errno = 0;
    return true;
  }

  if (dontwait && is_backpressure_errno(err)) {
    if (out_errno) *out_errno = err;
    return false;
  }

  close_fd(backend.ctrl_fd);
  backend.ctrl_fd = -1;
  backend.ctrl_fd = connect_unix_socket(backend);
  if (UNLIKELY(backend.ctrl_fd < 0)) {
    if (out_errno) *out_errno = err;
    return false;
  }
  init_backend_send_msg(backend);
  err = count == 1 ? send_passed_fd_prebuilt_errno(backend, fds[0], dontwait)
                   : send_passed_fds_errno(backend.ctrl_fd, fds, count, dontwait);
  if (LIKELY(err == 0)) {
    if (out_errno) *out_errno = 0;
    return true;
  }
  close_fd(backend.ctrl_fd);
  backend.ctrl_fd = -1;
  if (out_errno) *out_errno = err;
  return false;
}

bool send_to_backend_batch(Backend& backend, const int* fds, int count) {
  return send_to_backend_batch_mode(backend, fds, count, false, nullptr);
}

void close_batch(FdBatch& batch) {
  for (int i = 0; i < batch.count; ++i) {
    close_fd(batch.fds[i]);
  }
  batch.count = 0;
}

void flush_batch(Backend& backend, FdBatch& batch) {
  if (batch.count <= 0) return;
  send_to_backend_batch(backend, batch.fds, batch.count);
  close_batch(batch);
}

bool connect_all(Backend (&backends)[2]) {
  if (connect_backend(backends[0], g_initial_connect_retries) &&
      connect_backend(backends[1], g_initial_connect_retries)) {
    return true;
  }
  close_fd(backends[0].ctrl_fd);
  close_fd(backends[1].ctrl_fd);
  backends[0].ctrl_fd = -1;
  backends[1].ctrl_fd = -1;
  return false;
}

void close_backend(Backend& backend) {
  close_fd(backend.ctrl_fd);
  backend.ctrl_fd = -1;
}

void close_backends(std::vector<Backend>& backends) {
  for (Backend& backend : backends) {
    close_backend(backend);
  }
}

bool connect_backends(std::vector<Backend>& backends) {
  for (Backend& backend : backends) {
    if (!connect_backend(backend, g_initial_connect_retries)) {
      close_backends(backends);
      return false;
    }
  }
  return true;
}

std::vector<Backend> make_channel_backends(const Backend& target1, const Backend& target2) {
  std::vector<Backend> backends;
  backends.reserve(static_cast<size_t>(g_control_channels_per_target * 2));
  for (int i = 0; i < g_control_channels_per_target; ++i) {
    backends.push_back({target1.path, target1.path_len, -1});
    backends.push_back({target2.path, target2.path_len, -1});
  }
  return backends;
}

bool send_all(int fd, const char* data, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
    if (LIKELY(n > 0)) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (UNLIKELY(n < 0 && errno == EINTR)) continue;
    return false;
  }
  return true;
}

void run_self_warm() {
  if (g_self_warm_requests <= 0) return;
  if (g_self_warm_delay_ms > 0) {
    timespec ts{g_self_warm_delay_ms / 1000,
                (g_self_warm_delay_ms % 1000) * 1000 * 1000};
    nanosleep(&ts, nullptr);
  }

  static constexpr char kWarmBody[] =
      R"({"id":"lb-warmup","transaction":{"amount":5293.06,"installments":8,"requested_at":"2028-09-19T03:34:29Z"},"customer":{"avg_amount":60.14,"tx_count_24h":11,"known_merchants":["MERC-009","MERC-001"]},"merchant":{"id":"MERC-087","mcc":"7995","avg_amount":21.57},"terminal":{"is_online":false,"card_present":false,"km_from_home":265.78},"last_transaction":{"timestamp":"2024-01-04T03:43:32Z","km_from_current":722.93}})";
  char req[1024];
  const int req_len = std::snprintf(
      req, sizeof(req),
      "POST /fraud-score HTTP/1.1\r\nHost: localhost\r\nContent-Length:%zu\r\n\r\n%s",
      sizeof(kWarmBody) - 1, kWarmBody);
  if (req_len <= 0 || static_cast<size_t>(req_len) >= sizeof(req)) return;

  int ok = 0;
  int fail = 0;
  for (int i = 0; i < g_self_warm_requests; ++i) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
      ++fail;
      continue;
    }
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(g_listen_port));
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
      close_fd(fd);
      ++fail;
      continue;
    }
    if (!send_all(fd, req, static_cast<size_t>(req_len))) {
      close_fd(fd);
      ++fail;
      continue;
    }
    char buf[256];
    for (;;) {
      ssize_t n = recv(fd, buf, sizeof(buf), 0);
      if (n > 0) {
        ++ok;
        break;
      }
      if (n < 0 && errno == EINTR) continue;
      ++fail;
      break;
    }
    close_fd(fd);
  }
  std::fprintf(stderr, "fd lb self-warm finished requests=%d ok=%d fail=%d\n",
               g_self_warm_requests, ok, fail);
}

template <bool SetNoDelay, bool SetQuickAck>
void tune_accepted_client(int cfd) {
  int one = 1;
  if constexpr (SetNoDelay) {
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }
  if constexpr (SetQuickAck) {
    setsockopt(cfd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
  }
}

template <bool SetNoDelay, bool SetQuickAck>
void handoff_client(Backend& backend, int cfd) {
  tune_accepted_client<SetNoDelay, SetQuickAck>(cfd);
  if (UNLIKELY(!send_to_backend(backend, cfd))) {
    close_fd(cfd);
    return;
  }
  close_fd(cfd);
}

template <bool SetNoDelay, bool SetQuickAck>
void handoff_client_dontwait_any(Backend (&backends)[2], uint64_t first, int cfd) {
  tune_accepted_client<SetNoDelay, SetQuickAck>(cfd);

  int err0 = 0;
  const int fd_to_send = cfd;
  Backend& primary = backends[first & 1u];
  if (LIKELY(send_to_backend_batch_mode(primary, &fd_to_send, 1, true, &err0))) {
    close_fd(cfd);
    return;
  }

  if (is_backpressure_errno(err0)) {
    int err1 = 0;
    Backend& secondary = backends[(first ^ 1u) & 1u];
    if (send_to_backend_batch_mode(secondary, &fd_to_send, 1, true, &err1)) {
      close_fd(cfd);
      return;
    }
  }

  if (UNLIKELY(!send_to_backend(primary, cfd))) {
    Backend& secondary = backends[(first ^ 1u) & 1u];
    if (UNLIKELY(!send_to_backend(secondary, cfd))) {
      close_fd(cfd);
      return;
    }
  }
  close_fd(cfd);
}

template <bool SetNoDelay, bool SetQuickAck>
void handoff_client_dontwait_any(std::vector<Backend>& backends, uint64_t first, int cfd) {
  tune_accepted_client<SetNoDelay, SetQuickAck>(cfd);
  if (UNLIKELY(backends.empty())) {
    close_fd(cfd);
    return;
  }

  const int fd_to_send = cfd;
  const size_t n = backends.size();
  const size_t primary_idx = static_cast<size_t>(first % n);
  int err = 0;
  Backend& primary = backends[primary_idx];
  if (LIKELY(send_to_backend_batch_mode(primary, &fd_to_send, 1, true, &err))) {
    close_fd(cfd);
    return;
  }

  if (is_backpressure_errno(err)) {
    for (size_t off = 1; off < n; ++off) {
      int secondary_err = 0;
      Backend& secondary = backends[(primary_idx + off) % n];
      if (send_to_backend_batch_mode(secondary, &fd_to_send, 1, true, &secondary_err)) {
        close_fd(cfd);
        return;
      }
    }
  }

  if (send_to_backend(primary, cfd)) {
    close_fd(cfd);
    return;
  }
  for (size_t off = 1; off < n; ++off) {
    Backend& secondary = backends[(primary_idx + off) % n];
    if (send_to_backend(secondary, cfd)) {
      close_fd(cfd);
      return;
    }
  }
  close_fd(cfd);
}

template <bool SetNoDelay, bool SetQuickAck>
void queue_or_handoff_client(Backend& backend, FdBatch& batch, int cfd) {
  if (g_fd_batch <= 1) {
    handoff_client<SetNoDelay, SetQuickAck>(backend, cfd);
    return;
  }
  tune_accepted_client<SetNoDelay, SetQuickAck>(cfd);
  batch.fds[batch.count++] = cfd;
  if (batch.count >= g_fd_batch) {
    flush_batch(backend, batch);
  }
}

template <bool SetNoDelay, bool SetQuickAck, bool LocalRR>
void accept_loop_channels_impl(int listen_fd, std::vector<Backend>& backends, int worker_id,
                               std::atomic<uint64_t>* global_rr) {
  maybe_pin_worker_cpu(worker_id);
  uint64_t rr = static_cast<uint64_t>(worker_id);
  std::vector<FdBatch> batches(backends.size());
  pollfd pfd{listen_fd, POLLIN, 0};
  for (;;) {
    const uint64_t accept_t0 = g_lb_profile ? now_ns() : 0;
    int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (UNLIKELY(cfd < 0)) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (g_lb_profile) {
          g_lb_prof.accept_eagain.fetch_add(1, std::memory_order_relaxed);
        }
        for (size_t i = 0; i < backends.size(); ++i) {
          flush_batch(backends[i], batches[i]);
        }
        const uint64_t poll_t0 = g_lb_profile ? now_ns() : 0;
        do {
          pfd.revents = 0;
        } while (poll(&pfd, 1, -1) < 0 && errno == EINTR);
        if (g_lb_profile) {
          g_lb_prof.poll_waits.fetch_add(1, std::memory_order_relaxed);
          record_lb_ns(now_ns() - poll_t0, g_lb_prof.poll_wait_bins,
                       g_lb_prof.poll_wait_ns, g_lb_prof.poll_wait_max_ns);
        }
        continue;
      }
      if (errno == EINTR) continue;
      std::perror("fd lb accept");
      for (size_t i = 0; i < backends.size(); ++i) {
        flush_batch(backends[i], batches[i]);
      }
      break;
    }
    if (g_lb_profile) {
      g_lb_prof.accepts.fetch_add(1, std::memory_order_relaxed);
      record_lb_ns(now_ns() - accept_t0, g_lb_prof.accept_bins,
                   g_lb_prof.accept_ns, g_lb_prof.accept_max_ns);
      maybe_print_lb_profile();
    }

    uint64_t idx;
    if constexpr (LocalRR) {
      idx = rr++;
    } else {
      idx = global_rr ? global_rr->fetch_add(1, std::memory_order_relaxed) : rr++;
    }
    if (g_control_dontwait && g_fd_batch <= 1) {
      handoff_client_dontwait_any<SetNoDelay, SetQuickAck>(backends, idx, cfd);
    } else {
      const size_t target = static_cast<size_t>(idx % backends.size());
      queue_or_handoff_client<SetNoDelay, SetQuickAck>(backends[target], batches[target], cfd);
    }
  }
}

void accept_loop_channels(int listen_fd, std::vector<Backend>& backends, int worker_id,
                          std::atomic<uint64_t>* global_rr, bool local_rr) {
  if (g_set_accept_nodelay) {
    if (g_set_accept_quickack) {
      if (local_rr) {
        accept_loop_channels_impl<true, true, true>(listen_fd, backends, worker_id, global_rr);
      } else {
        accept_loop_channels_impl<true, true, false>(listen_fd, backends, worker_id, global_rr);
      }
    } else {
      if (local_rr) {
        accept_loop_channels_impl<true, false, true>(listen_fd, backends, worker_id, global_rr);
      } else {
        accept_loop_channels_impl<true, false, false>(listen_fd, backends, worker_id, global_rr);
      }
    }
  } else {
    if (g_set_accept_quickack) {
      if (local_rr) {
        accept_loop_channels_impl<false, true, true>(listen_fd, backends, worker_id, global_rr);
      } else {
        accept_loop_channels_impl<false, true, false>(listen_fd, backends, worker_id, global_rr);
      }
    } else {
      if (local_rr) {
        accept_loop_channels_impl<false, false, true>(listen_fd, backends, worker_id, global_rr);
      } else {
        accept_loop_channels_impl<false, false, false>(listen_fd, backends, worker_id, global_rr);
      }
    }
  }
}

template <bool SetNoDelay, bool SetQuickAck>
void accept_loop_single_impl(int listen_fd, Backend (&backends)[2]) {
  uint64_t rr = 0;
  FdBatch batches[2]{};
  pollfd pfd{listen_fd, POLLIN, 0};
  for (;;) {
    const uint64_t accept_t0 = g_lb_profile ? now_ns() : 0;
    int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (UNLIKELY(cfd < 0)) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (g_lb_profile) {
          g_lb_prof.accept_eagain.fetch_add(1, std::memory_order_relaxed);
        }
        flush_batch(backends[0], batches[0]);
        flush_batch(backends[1], batches[1]);
        const uint64_t poll_t0 = g_lb_profile ? now_ns() : 0;
        do {
          pfd.revents = 0;
        } while (poll(&pfd, 1, -1) < 0 && errno == EINTR);
        if (g_lb_profile) {
          g_lb_prof.poll_waits.fetch_add(1, std::memory_order_relaxed);
          record_lb_ns(now_ns() - poll_t0, g_lb_prof.poll_wait_bins,
                       g_lb_prof.poll_wait_ns, g_lb_prof.poll_wait_max_ns);
        }
        continue;
      }
      if (errno == EINTR) continue;
      std::perror("fd lb accept");
      flush_batch(backends[0], batches[0]);
      flush_batch(backends[1], batches[1]);
      break;
    }
    if (g_lb_profile) {
      g_lb_prof.accepts.fetch_add(1, std::memory_order_relaxed);
      record_lb_ns(now_ns() - accept_t0, g_lb_prof.accept_bins,
                   g_lb_prof.accept_ns, g_lb_prof.accept_max_ns);
      maybe_print_lb_profile();
    }

    const uint64_t idx = rr++ & 1;
    if (g_control_dontwait && g_fd_batch <= 1) {
      handoff_client_dontwait_any<SetNoDelay, SetQuickAck>(backends, idx, cfd);
    } else {
      queue_or_handoff_client<SetNoDelay, SetQuickAck>(backends[idx], batches[idx], cfd);
    }
  }
}

template <bool SetNoDelay, bool SetQuickAck>
void accept_loop_single_blocking_impl(int listen_fd, Backend (&backends)[2]) {
  uint64_t rr = 0;
  for (;;) {
    const uint64_t accept_t0 = g_lb_profile ? now_ns() : 0;
    int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (UNLIKELY(cfd < 0)) {
      if (errno == EINTR) continue;
      std::perror("fd lb accept");
      break;
    }
    if (g_lb_profile) {
      g_lb_prof.accepts.fetch_add(1, std::memory_order_relaxed);
      record_lb_ns(now_ns() - accept_t0, g_lb_prof.accept_bins,
                   g_lb_prof.accept_ns, g_lb_prof.accept_max_ns);
      maybe_print_lb_profile();
    }

    Backend& backend = backends[rr++ & 1];
    handoff_client<SetNoDelay, SetQuickAck>(backend, cfd);
  }
}

void accept_loop_single(int listen_fd, Backend (&backends)[2]) {
  maybe_pin_worker_cpu(0);
  if (g_set_accept_nodelay) {
    if (g_set_accept_quickack) {
      if (g_blocking_accept) {
        accept_loop_single_blocking_impl<true, true>(listen_fd, backends);
      } else {
        accept_loop_single_impl<true, true>(listen_fd, backends);
      }
    } else {
      if (g_blocking_accept) {
        accept_loop_single_blocking_impl<true, false>(listen_fd, backends);
      } else {
        accept_loop_single_impl<true, false>(listen_fd, backends);
      }
    }
  } else {
    if (g_set_accept_quickack) {
      if (g_blocking_accept) {
        accept_loop_single_blocking_impl<false, true>(listen_fd, backends);
      } else {
        accept_loop_single_impl<false, true>(listen_fd, backends);
      }
    } else {
      if (g_blocking_accept) {
        accept_loop_single_blocking_impl<false, false>(listen_fd, backends);
      } else {
        accept_loop_single_impl<false, false>(listen_fd, backends);
      }
    }
  }
}

template <bool SetNoDelay, bool SetQuickAck, bool LocalRR>
void accept_loop_worker_impl(WorkerConfig config) {
  maybe_pin_worker_cpu(config.worker_id);
  Backend backends[2] = {config.target1, config.target2};
  if (!connect_all(backends)) {
    std::fprintf(stderr, "fd lb worker %d failed to connect control sockets\n", config.worker_id);
    return;
  }

  uint64_t local_rr = static_cast<uint64_t>(config.worker_id);
  FdBatch batches[2]{};
  pollfd pfd{config.listen_fd, POLLIN, 0};
  for (;;) {
    const uint64_t accept_t0 = g_lb_profile ? now_ns() : 0;
    int cfd = accept4(config.listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (UNLIKELY(cfd < 0)) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        if (g_lb_profile) {
          g_lb_prof.accept_eagain.fetch_add(1, std::memory_order_relaxed);
        }
        flush_batch(backends[0], batches[0]);
        flush_batch(backends[1], batches[1]);
        const uint64_t poll_t0 = g_lb_profile ? now_ns() : 0;
        do {
          pfd.revents = 0;
        } while (poll(&pfd, 1, -1) < 0 && errno == EINTR);
        if (g_lb_profile) {
          g_lb_prof.poll_waits.fetch_add(1, std::memory_order_relaxed);
          record_lb_ns(now_ns() - poll_t0, g_lb_prof.poll_wait_bins,
                       g_lb_prof.poll_wait_ns, g_lb_prof.poll_wait_max_ns);
        }
        continue;
      }
      if (errno == EINTR) continue;
      std::perror("fd lb accept");
      flush_batch(backends[0], batches[0]);
      flush_batch(backends[1], batches[1]);
      break;
    }
    if (g_lb_profile) {
      g_lb_prof.accepts.fetch_add(1, std::memory_order_relaxed);
      record_lb_ns(now_ns() - accept_t0, g_lb_prof.accept_bins,
                   g_lb_prof.accept_ns, g_lb_prof.accept_max_ns);
      maybe_print_lb_profile();
    }

    uint64_t idx;
    if constexpr (LocalRR) {
      idx = local_rr++;
    } else {
      idx = config.rr->fetch_add(1, std::memory_order_relaxed);
    }
    const uint64_t target = idx & 1;
    if (g_control_dontwait && g_fd_batch <= 1) {
      handoff_client_dontwait_any<SetNoDelay, SetQuickAck>(backends, target, cfd);
    } else {
      queue_or_handoff_client<SetNoDelay, SetQuickAck>(backends[target], batches[target], cfd);
    }
  }

  flush_batch(backends[0], batches[0]);
  flush_batch(backends[1], batches[1]);
  close_fd(backends[0].ctrl_fd);
  close_fd(backends[1].ctrl_fd);
}

void accept_loop_worker(WorkerConfig config) {
  if (g_control_channels_per_target > 1) {
    std::vector<Backend> channel_backends =
        make_channel_backends(config.target1, config.target2);
    if (!connect_backends(channel_backends)) {
      std::fprintf(stderr, "fd lb worker %d failed to connect channelized control sockets\n",
                   config.worker_id);
      return;
    }
    accept_loop_channels(config.listen_fd, channel_backends, config.worker_id, config.rr,
                         config.local_rr);
    close_backends(channel_backends);
    return;
  }

  if (g_set_accept_nodelay) {
    if (g_set_accept_quickack) {
      if (config.local_rr) {
        accept_loop_worker_impl<true, true, true>(config);
      } else {
        accept_loop_worker_impl<true, true, false>(config);
      }
    } else {
      if (config.local_rr) {
        accept_loop_worker_impl<true, false, true>(config);
      } else {
        accept_loop_worker_impl<true, false, false>(config);
      }
    }
  } else {
    if (g_set_accept_quickack) {
      if (config.local_rr) {
        accept_loop_worker_impl<false, true, true>(config);
      } else {
        accept_loop_worker_impl<false, true, false>(config);
      }
    } else {
      if (config.local_rr) {
        accept_loop_worker_impl<false, false, true>(config);
      } else {
        accept_loop_worker_impl<false, false, false>(config);
      }
    }
  }
}

}  // namespace

int main() {
  const char* target1 = std::getenv("FD_TARGET_1");
  const char* target2 = std::getenv("FD_TARGET_2");
  if (!target1 || !target2 || target1[0] == '\0' || target2[0] == '\0') {
    std::fprintf(stderr, "fd lb missing FD_TARGET_1/FD_TARGET_2\n");
    return 1;
  }
  const size_t target1_len = std::strlen(target1);
  const size_t target2_len = std::strlen(target2);
  if (target1_len >= kUnixPathMax || target2_len >= kUnixPathMax) {
    std::fprintf(stderr, "fd lb target path too long\n");
    return 1;
  }

  int workers = env_int("FD_LB_WORKERS", 1, 1, 8);
  bool reuse_port = workers > 1 && env_bool("FD_LB_REUSEPORT", false);
  bool local_rr = workers > 1 && env_bool("FD_LB_LOCAL_RR", false);
  g_blocking_accept = workers == 1 && env_bool("FD_LB_BLOCKING_ACCEPT", false);
  g_defer_accept_seconds = env_int("FD_LB_DEFER_ACCEPT_SECONDS", 1, 0, 60);
  g_initial_connect_retries = env_int("FD_LB_CONNECT_RETRIES", 3000, 1, 30000);
  g_reconnect_retries = env_int("FD_LB_RECONNECT_RETRIES", 3, 0, 3000);
  g_connect_sleep_ms = env_int("FD_LB_CONNECT_SLEEP_MS", 10, 0, 1000);
  g_tcp_rcvbuf = env_int("FD_LB_TCP_RCVBUF", 0, 0, 4 * 1024 * 1024);
  g_tcp_sndbuf = env_int("FD_LB_TCP_SNDBUF", 0, 0, 4 * 1024 * 1024);
  g_control_rcvbuf = env_int("FD_LB_CONTROL_RCVBUF", 0, 0, 1024 * 1024);
  g_control_sndbuf = env_int("FD_LB_CONTROL_SNDBUF", 0, 0, 1024 * 1024);
  g_set_accept_nodelay = env_bool("FD_LB_SET_ACCEPT_NODELAY", false);
  g_set_accept_quickack = env_bool("FD_LB_SET_ACCEPT_QUICKACK", false);
  g_self_warm_requests = env_int("FD_LB_SELF_WARM_REQUESTS", 0, 0, 10000);
  g_self_warm_delay_ms = env_int("FD_LB_SELF_WARM_DELAY_MS", 20, 0, 10000);
  g_socket_busy_poll_us = env_int("FD_LB_SOCKET_BUSY_POLL_US", 0, 0, 1000);
  g_socket_busy_poll_budget = env_int("FD_LB_SOCKET_BUSY_POLL_BUDGET", 0, 0, 256);
  g_socket_prefer_busy_poll = env_bool("FD_LB_SOCKET_PREFER_BUSY_POLL", false);
  g_timer_slack_ns = env_int("FD_LB_TIMER_SLACK_NS", 0, 0, 1000000);
  g_fd_batch = env_int("FD_LB_FD_BATCH", 1, 1, kMaxFdBatch);
  g_tcp_fastopen = env_int("FD_LB_TCP_FASTOPEN", 0, 0, 4096);
  g_control_dontwait = env_bool("FD_LB_CONTROL_DONTWAIT", false);
  g_control_seqpacket = env_bool("FD_LB_CONTROL_SEQPACKET", false);
  g_lb_profile = env_bool("FD_LB_PROFILE", false);
  g_pin_workers = env_bool("FD_LB_PIN_WORKERS", false);
  g_control_channels_per_target = env_int("FD_LB_CHANNELS_PER_TARGET", 1, 1, 8);
  if (g_lb_profile) {
    std::atexit(print_lb_profile);
  }
  if (g_timer_slack_ns > 0) {
    prctl(PR_SET_TIMERSLACK, g_timer_slack_ns, 0, 0, 0);
  }

  int listen_fd = create_tcp_listener(std::getenv("FD_LB_ADDR"), reuse_port);
  if (listen_fd < 0) {
    std::perror("fd lb listen");
    return 1;
  }

  Backend backends[2] = {{target1, target1_len, -1}, {target2, target2_len, -1}};
  std::vector<Backend> channel_backends;
  if (workers == 1 && g_control_channels_per_target > 1) {
    channel_backends = make_channel_backends(backends[0], backends[1]);
    if (!connect_backends(channel_backends)) {
      std::fprintf(stderr, "fd lb failed to connect channelized control sockets\n");
      close_fd(listen_fd);
      return 1;
    }
  } else if (workers == 1 && !connect_all(backends)) {
    std::fprintf(stderr, "fd lb failed to connect persistent control sockets\n");
    close_fd(listen_fd);
    return 1;
  }

  std::fprintf(stderr,
               "native fd-passing lb running on %s -> [%s, %s] persistent-control workers=%d reuseport=%d local-rr=%d pin-workers=%d channels-per-target=%d defer=%d connect-retries=%d reconnect-retries=%d sleep-ms=%d tcp-rcvbuf=%d tcp-sndbuf=%d control-rcvbuf=%d control-sndbuf=%d accept-nodelay=%d accept-quickack=%d blocking-accept=%d fd-batch=%d tcp-fastopen=%d control-dontwait=%d control-seqpacket=%d profile=%d\n",
               std::getenv("FD_LB_ADDR") ? std::getenv("FD_LB_ADDR") : "0.0.0.0:9999",
               target1, target2, workers, reuse_port ? 1 : 0, local_rr ? 1 : 0,
               g_pin_workers ? 1 : 0, g_control_channels_per_target, g_defer_accept_seconds,
               g_initial_connect_retries, g_reconnect_retries, g_connect_sleep_ms,
               g_tcp_rcvbuf, g_tcp_sndbuf, g_control_rcvbuf, g_control_sndbuf,
               g_set_accept_nodelay ? 1 : 0, g_set_accept_quickack ? 1 : 0,
               g_blocking_accept ? 1 : 0, g_fd_batch,
               g_tcp_fastopen,
               g_control_dontwait ? 1 : 0, g_control_seqpacket ? 1 : 0,
               g_lb_profile ? 1 : 0);

  if (g_self_warm_requests > 0) {
    std::thread(run_self_warm).detach();
  }

  if (workers == 1) {
    if (g_control_channels_per_target > 1) {
      accept_loop_channels(listen_fd, channel_backends, 0, nullptr, true);
    } else {
      accept_loop_single(listen_fd, backends);
    }
  } else {
    std::atomic<uint64_t> rr{0};
    std::vector<int> listen_fds;
    listen_fds.reserve(static_cast<size_t>(workers));
    listen_fds.push_back(listen_fd);
    if (reuse_port) {
      for (int i = 1; i < workers; ++i) {
        int fd = create_tcp_listener(std::getenv("FD_LB_ADDR"), true);
        if (fd < 0) {
          std::perror("fd lb reuseport listen");
          for (int extra_fd : listen_fds) close_fd(extra_fd);
          return 1;
        }
        listen_fds.push_back(fd);
      }
    }

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(workers));
    for (int i = 0; i < workers; ++i) {
      WorkerConfig config{listen_fds[reuse_port ? i : 0],
                          {target1, target1_len, -1},
                          {target2, target2_len, -1},
                          &rr,
                          i,
                          local_rr};
      threads.emplace_back(accept_loop_worker, config);
    }
    for (auto& thread : threads) {
      thread.join();
    }
    for (size_t i = 1; i < listen_fds.size(); ++i) {
      close_fd(listen_fds[i]);
    }
  }

  if (!channel_backends.empty()) {
    close_backends(channel_backends);
  }
  close_fd(backends[0].ctrl_fd);
  close_fd(backends[1].ctrl_fd);
  close_fd(listen_fd);
  return 1;
}
