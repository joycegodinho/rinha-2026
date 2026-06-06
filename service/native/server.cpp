#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/io_uring.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <sched.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "native_ivf.hpp"
#include "native_vector.hpp"

#ifndef PURE_CPP_IVF
#include "libclassifier.h"
#else
extern "C" int fraud_init() {
  return 1;
}
extern "C" int fraud_classify(uint8_t*, size_t) {
  return -1;
}
extern "C" int fraud_classify_vector(float*) {
  return -1;
}
extern "C" void fraud_profile_report() {}
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
#ifndef TCP_DEFER_ACCEPT
#define TCP_DEFER_ACCEPT 9
#endif
#ifndef TCP_NOTSENT_LOWAT
#define TCP_NOTSENT_LOWAT 25
#endif

namespace {

#if ENABLE_BRANCH_HINTS && (defined(__GNUC__) || defined(__clang__))
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#ifndef EPIOCSPARAMS
#ifndef EPOLL_IOC_TYPE
#define EPOLL_IOC_TYPE 0x8A
#endif
struct epoll_params {
  uint32_t busy_poll_usecs;
  uint16_t busy_poll_budget;
  uint8_t prefer_busy_poll;
  uint8_t __pad;
};
#define EPIOCSPARAMS _IOW(EPOLL_IOC_TYPE, 0x01, struct epoll_params)
#endif

bool g_fake_classifier = false;
bool g_cpp_vector_parser = false;
bool g_cpp_ivf = false;
bool g_ivf_trace = false;
bool g_fd_receiver = false;
bool g_profile = false;
bool g_tail_profile = false;
bool g_event_profile = false;
bool g_skip_fd_nonblocking = true;
bool g_aggressive_compact = true;
bool g_fragment_profile = false;
bool g_rw_syscalls = false;
bool g_direct_static_response = false;
bool g_direct_complete_post = false;
bool g_native_fast_path = false;
bool g_native_tier_path = false;
bool g_fd_tcp_quickack = false;
bool g_fd_control_single_recv = false;
bool g_fd_client_level_trigger = false;
bool g_fd_direct_slots = false;
bool g_mlockall = false;
bool g_tcp_reuseport = false;
int g_native_fast_path_mode = 0;
bool g_native_fast_fraud_ivf_route = false;
int g_native_fast_fraud_ivf_quick_probe = 4;
volatile sig_atomic_t g_stop = 0;
NativeIVF g_native_ivf;
int g_tcp_rcvbuf = 0;
int g_tcp_sndbuf = 0;
int g_control_rcvbuf = 0;
int g_control_sndbuf = 0;
bool g_control_seqpacket = false;
bool g_control_recv_dontwait = false;
bool g_control_recv_cmsg_cloexec = false;
bool g_control_loop_warn = false;
bool g_epoll_interest_cache = false;
int g_fd_control_connections = 1;
int g_epoll_busy_poll_us = 0;
int g_epoll_busy_poll_budget = 8;
int g_epoll_prefer_busy_poll = 1;
int g_epoll_spin_us = 0;
int g_epoll_idle_us = 0;
int g_epoll_idle_max_us = 0;
int g_epoll_idle_active_window_us = 0;
int g_epoll_timeout_backoff_after = 0;
int g_epoll_timeout_backoff_short_waits = 1;
int g_startup_warmup_requests = 0;
int g_fd_initial_read_spins = 0;
int g_post_flush_read_spins = 0;
int g_timer_slack_ns = 0;
int g_sched_fifo_priority = 0;
int g_nice_value = 0;
bool g_set_nice_value = false;
int g_tcp_workers = 1;
int g_tcp_defer_accept_seconds = 0;
int g_tcp_socket_busy_poll_us = 0;
int g_tcp_socket_busy_poll_budget = 0;
int g_tcp_socket_prefer_busy_poll = 0;
int g_tcp_client_busy_poll = 0;
int g_tcp_notsent_lowat = 0;
bool g_tcp_listener_nodelay = true;
bool g_tcp_accept_nodelay = true;
bool g_direct_skip_accept4_nonblocking = true;
bool g_direct_poll_service = false;
bool g_single_recv_per_event = false;
bool g_stop_parse_on_empty = false;
bool g_io_uring_direct = false;
int g_tcp_self_warm_requests = 0;
int g_tcp_self_warm_delay_ms = 20;
pid_t g_tcp_self_warm_pid = -1;
bool g_tcp_self_warm_complete = true;

struct FragmentCounters {
  uint64_t read_calls = 0;
  uint64_t read_data = 0;
  uint64_t read_empty = 0;
  uint64_t read_closed = 0;
  uint64_t read_full = 0;
  uint64_t read_bytes = 0;
  uint64_t drain_calls = 0;
  uint64_t drain_data_calls = 0;
  uint64_t max_reads_per_drain = 0;
  uint64_t parse_waits = 0;
  uint64_t fast_header_incomplete = 0;
  uint64_t fallback_header_incomplete = 0;
  uint64_t body_incomplete = 0;
  uint64_t partial_lt_128 = 0;
  uint64_t partial_lt_512 = 0;
  uint64_t partial_lt_1024 = 0;
  uint64_t partial_ge_1024 = 0;
  uint64_t max_partial_bytes = 0;
};

alignas(64) FragmentCounters g_frag{};

constexpr size_t kProfileBucketCount = 7;
constexpr std::array<uint64_t, kProfileBucketCount> kProfileBucketLimitsNs = {
    50000, 100000, 200000, 500000, 1000000, 2000000, static_cast<uint64_t>(-1)};
constexpr std::array<std::string_view, kProfileBucketCount> kProfileBucketLabels = {
    "<50us", "<100us", "<200us", "<500us", "<1ms", "<2ms", ">=2ms"};

struct ProfileCounters {
  uint64_t requests = 0;
  uint64_t body_bytes = 0;
  uint64_t classify_ns = 0;
  uint64_t vector_ns = 0;
  uint64_t bridge_ns = 0;
  uint64_t go_body_ns = 0;
  uint64_t fake_ns = 0;
  uint64_t vector_failures = 0;
  uint64_t max_classify_ns = 0;
  uint64_t max_vector_ns = 0;
  uint64_t max_bridge_ns = 0;
  uint64_t fast_post_requests = 0;
  uint64_t fallback_post_requests = 0;
  uint64_t ready_requests = 0;
  uint64_t bad_requests = 0;
  uint64_t other_requests = 0;
  uint64_t native_fast_path_hits = 0;
  uint64_t native_fast_path_legit = 0;
  uint64_t native_fast_path_fraud = 0;
  uint64_t native_tier_path_hits = 0;
  uint64_t direct_complete_post_hits = 0;
  std::array<uint64_t, 6> native_fast_path_scores{};
  std::array<uint64_t, kProfileBucketCount> classify_bins{};
  std::array<uint64_t, kProfileBucketCount> vector_bins{};
  std::array<uint64_t, kProfileBucketCount> bridge_bins{};
};

alignas(64) ProfileCounters g_prof{};

constexpr size_t kTailProfileBucketCount = 10;
constexpr std::array<uint64_t, kTailProfileBucketCount> kTailProfileBucketLimitsNs = {
    25000, 50000, 100000, 200000, 350000, 500000, 750000, 1000000, 2000000,
    static_cast<uint64_t>(-1)};
constexpr std::array<std::string_view, kTailProfileBucketCount> kTailProfileBucketLabels = {
    "<25us", "<50us", "<100us", "<200us", "<350us", "<500us", "<750us",
    "<1ms", "<2ms", ">=2ms"};

struct TailProfileCounters {
  uint64_t parse_loops = 0;
  uint64_t parse_loop_ns = 0;
  uint64_t parse_loop_max_ns = 0;
  uint64_t flush_calls = 0;
  uint64_t flush_ns = 0;
  uint64_t flush_max_ns = 0;
  uint64_t client_events = 0;
  uint64_t client_event_ns = 0;
  uint64_t client_event_max_ns = 0;
  std::array<uint64_t, kTailProfileBucketCount> parse_loop_bins{};
  std::array<uint64_t, kTailProfileBucketCount> flush_bins{};
  std::array<uint64_t, kTailProfileBucketCount> client_event_bins{};
};

alignas(64) TailProfileCounters g_tail{};

constexpr size_t kEventProfileBucketCount = 8;
constexpr std::array<uint64_t, kEventProfileBucketCount> kEventProfileBucketLimitsNs = {
    10000, 25000, 50000, 100000, 200000, 500000, 1000000,
    static_cast<uint64_t>(-1)};
constexpr std::array<std::string_view, kEventProfileBucketCount> kEventProfileBucketLabels = {
    "<10us", "<25us", "<50us", "<100us", "<200us", "<500us", "<1ms", ">=1ms"};

constexpr size_t kEpollBatchBucketCount = 12;
constexpr std::array<uint64_t, kEpollBatchBucketCount> kEpollBatchBucketLimits = {
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, static_cast<uint64_t>(-1)};
constexpr std::array<std::string_view, kEpollBatchBucketCount> kEpollBatchBucketLabels = {
    "1", "2", "<=4", "<=8", "<=16", "<=32", "<=64", "<=128", "<=256",
    "<=512", "<=1024", ">1024"};

struct EventProfileCounters {
  uint64_t epoll_waits = 0;
  uint64_t epoll_wakeups = 0;
  uint64_t epoll_timeouts = 0;
  uint64_t epoll_short_waits = 0;
  uint64_t epoll_long_waits = 0;
  uint64_t epoll_events = 0;
  uint64_t epoll_max_batch = 0;
  uint64_t listen_events = 0;
  uint64_t control_events = 0;
  uint64_t control_recv_calls = 0;
  uint64_t control_got = 0;
  uint64_t control_empty = 0;
  uint64_t control_errors = 0;
  uint64_t control_loop_iters = 0;
  uint64_t control_loop_max_iters = 0;
  uint64_t control_loop_over5 = 0;
  uint64_t client_events = 0;
  uint64_t fd_clients = 0;
  uint64_t initial_read_clients = 0;
  uint64_t initial_read_data = 0;
  uint64_t initial_read_empty = 0;
  uint64_t initial_read_closed = 0;
  uint64_t initial_read_bad = 0;
  uint64_t post_flush_attempts = 0;
  uint64_t post_flush_data = 0;
  uint64_t post_flush_empty = 0;
  uint64_t post_flush_closed = 0;
  uint64_t post_flush_bad = 0;
  uint64_t post_flush_responses = 0;
  uint64_t epoll_mod_calls = 0;
  uint64_t epoll_mod_skipped = 0;
  uint64_t epoll_mod_errors = 0;
  uint64_t first_request_count = 0;
  uint64_t first_request_wait_ns = 0;
  uint64_t first_request_wait_max_ns = 0;
  std::array<uint64_t, kEventProfileBucketCount> first_request_wait_bins{};
  std::array<uint64_t, kEpollBatchBucketCount> epoll_batch_bins{};
};

alignas(64) EventProfileCounters g_event_prof{};

constexpr int kMaxEvents = 2048;
constexpr int kMaxConnections = 4096;
constexpr int kReadBufSize = 8192;
constexpr int kWriteBufSize = 4096;
constexpr int kBacklog = 4096;
constexpr int kMaxControlFdBatch = 16;
constexpr int kMaxControlConnections = 16;
constexpr uint32_t kReadEvents = EPOLLIN | EPOLLRDHUP | EPOLLET;
constexpr uint32_t kReadWriteEvents = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
constexpr uint32_t kLevelReadEvents = EPOLLIN | EPOLLRDHUP;
constexpr uint32_t kLevelReadWriteEvents = EPOLLIN | EPOLLOUT | EPOLLRDHUP;
constexpr uint64_t kListenEventId = 0;
constexpr uint64_t kControlEventBase = 1;

uint32_t client_read_events() {
  return g_fd_client_level_trigger ? kLevelReadEvents : kReadEvents;
}

uint32_t client_readwrite_events() {
  return g_fd_client_level_trigger ? kLevelReadWriteEvents : kReadWriteEvents;
}

constexpr std::string_view kGetReadyLine = "GET /ready HTTP/1.1";
constexpr std::string_view kPostFraudLine = "POST /fraud-score HTTP/1.1";
constexpr std::string_view kReadyKeepAlive =
    "HTTP/1.1 200 \r\nContent-Length:0\r\n\r\n";
constexpr std::string_view kReadyClose =
    "HTTP/1.1 200 \r\nContent-Length:0\r\nConnection: close\r\n\r\n";
constexpr std::string_view kReadyUnavailable =
    "HTTP/1.1 503 \r\nContent-Length:0\r\n\r\n";
constexpr std::string_view k404KeepAlive =
    "HTTP/1.1 404 \r\nContent-Length:0\r\nConnection: keep-alive\r\n\r\n";
constexpr std::string_view k404Close =
    "HTTP/1.1 404 \r\nContent-Length:0\r\nConnection: close\r\n\r\n";
constexpr std::string_view k405KeepAlive =
    "HTTP/1.1 405 \r\nContent-Length:0\r\nConnection: keep-alive\r\n\r\n";
constexpr std::string_view k405Close =
    "HTTP/1.1 405 \r\nContent-Length:0\r\nConnection: close\r\n\r\n";
constexpr std::string_view k400Close =
    "HTTP/1.1 400 \r\nContent-Length:0\r\nConnection: close\r\n\r\n";

constexpr std::array<std::string_view, 6> kFraudKeepAlive = {
    "HTTP/1.1 200 \r\nContent-Length:33\r\n\r\n{\"approved\":true,\"fraud_score\":0}",
    "HTTP/1.1 200 \r\nContent-Length:35\r\n\r\n{\"approved\":true,\"fraud_score\":0.2}",
    "HTTP/1.1 200 \r\nContent-Length:35\r\n\r\n{\"approved\":true,\"fraud_score\":0.4}",
    "HTTP/1.1 200 \r\nContent-Length:36\r\n\r\n{\"approved\":false,\"fraud_score\":0.6}",
    "HTTP/1.1 200 \r\nContent-Length:36\r\n\r\n{\"approved\":false,\"fraud_score\":0.8}",
    "HTTP/1.1 200 \r\nContent-Length:34\r\n\r\n{\"approved\":false,\"fraud_score\":1}",
};

constexpr std::array<std::string_view, 6> kFraudClose = {
    "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length:33\r\nConnection: close\r\n\r\n{\"approved\":true,\"fraud_score\":0}",
    "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length:35\r\nConnection: close\r\n\r\n{\"approved\":true,\"fraud_score\":0.2}",
    "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length:35\r\nConnection: close\r\n\r\n{\"approved\":true,\"fraud_score\":0.4}",
    "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length:36\r\nConnection: close\r\n\r\n{\"approved\":false,\"fraud_score\":0.6}",
    "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length:36\r\nConnection: close\r\n\r\n{\"approved\":false,\"fraud_score\":0.8}",
    "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length:34\r\nConnection: close\r\n\r\n{\"approved\":false,\"fraud_score\":1}",
};

constexpr std::array<std::string_view, 6> kStartupWarmupPayloads = {
    R"({"id":"warmup-1","transaction":{"amount":441.59,"installments":1,"requested_at":"2027-07-09T16:31:06Z"},"customer":{"avg_amount":883.18,"tx_count_24h":1,"known_merchants":["MERC-004","MERC-017"]},"merchant":{"id":"MERC-004","mcc":"5411","avg_amount":302.78},"terminal":{"is_online":false,"card_present":true,"km_from_home":33.88},"last_transaction":{"timestamp":"2027-06-04T14:14:22Z","km_from_current":18.43}})",
    R"({"id":"warmup-2","transaction":{"amount":5293.06,"installments":8,"requested_at":"2028-09-19T03:34:29Z"},"customer":{"avg_amount":60.14,"tx_count_24h":11,"known_merchants":["MERC-009","MERC-001"]},"merchant":{"id":"MERC-087","mcc":"7995","avg_amount":21.57},"terminal":{"is_online":false,"card_present":false,"km_from_home":265.78},"last_transaction":{"timestamp":"2024-01-04T03:43:32Z","km_from_current":722.93}})",
    R"({"id":"warmup-3","transaction":{"amount":7318.26,"installments":8,"requested_at":"2028-07-05T03:41:22Z"},"customer":{"avg_amount":158.57,"tx_count_24h":11,"known_merchants":["MERC-013","MERC-010"]},"merchant":{"id":"MERC-073","mcc":"7801","avg_amount":37.46},"terminal":{"is_online":true,"card_present":false,"km_from_home":417.33},"last_transaction":null})",
    R"({"customer":{"avg_amount":68.88,"tx_count_24h":18,"known_merchants":["MERC-004","MERC-015","MERC-007"]},"id":"warmup-4","last_transaction":{"timestamp":"2026-03-17T01:58:06Z","km_from_current":660.92},"merchant":{"id":"MERC-062","mcc":"7801","avg_amount":25.55},"terminal":{"is_online":true,"card_present":false,"km_from_home":881.61},"transaction":{"amount":4368.82,"installments":8,"requested_at":"2026-03-17T02:04:06Z"}})",
    R"({"id":"warmup-5","transaction":{"amount":29.47,"installments":2,"requested_at":"2028-12-24T08:34:05Z"},"customer":{"avg_amount":58.94,"tx_count_24h":3,"known_merchants":["MERC-004","MERC-014"]},"merchant":{"id":"MERC-014","mcc":"5411","avg_amount":378.62},"terminal":{"is_online":false,"card_present":true,"km_from_home":20.36},"last_transaction":{"timestamp":"2027-11-28T15:22:55Z","km_from_current":16.71}})",
    R"({"id":"warmup-6","transaction":{"amount":9797.7,"installments":7,"requested_at":"2026-11-14T06:09:00Z"},"customer":{"avg_amount":99.49,"tx_count_24h":13,"known_merchants":["MERC-006","MERC-014","MERC-013"]},"merchant":{"id":"MERC-094","mcc":"7802","avg_amount":33.01},"terminal":{"is_online":false,"card_present":true,"km_from_home":396.12},"last_transaction":{"timestamp":"2026-03-18T15:14:27Z","km_from_current":712.42}})",
};

struct alignas(4096) ConnectionBuffers {
  std::array<char, kReadBufSize> inbuf{};
  std::array<char, kWriteBufSize> outbuf{};
};
static_assert(sizeof(ConnectionBuffers) == kReadBufSize + kWriteBufSize);

struct Connection {
  int fd = -1;
  size_t in_start = 0;
  size_t in_end = 0;
  size_t out_used = 0;
  size_t out_sent = 0;
  const char* out_static = nullptr;
  bool close_after_write = false;
  bool active = false;
  bool first_request_recorded = false;
  uint32_t generation = 0;
  uint32_t epoll_events = 0;
  uint64_t accepted_ns = 0;
  ConnectionBuffers* buffers = nullptr;
};

int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void close_fd(int fd) {
  if (LIKELY(fd >= 0)) {
    close(fd);
  }
}

uint64_t now_ns();

void reset_connection(Connection& conn, int fd) {
  conn.fd = fd;
  conn.in_start = 0;
  conn.in_end = 0;
  conn.out_used = 0;
  conn.out_sent = 0;
  conn.out_static = nullptr;
  conn.close_after_write = false;
  conn.active = true;
  conn.first_request_recorded = false;
  ++conn.generation;
  if (conn.generation == 0) conn.generation = 1;
  conn.epoll_events = 0;
  conn.accepted_ns = g_event_profile ? now_ns() : 0;
}

void clear_connection(Connection& conn) {
  conn.fd = -1;
  conn.in_start = 0;
  conn.in_end = 0;
  conn.out_used = 0;
  conn.out_sent = 0;
  conn.out_static = nullptr;
  conn.close_after_write = false;
  conn.active = false;
  conn.first_request_recorded = false;
  conn.epoll_events = 0;
  conn.accepted_ns = 0;
}

uint64_t make_client_event_id(int slot, uint32_t generation) {
  return (static_cast<uint64_t>(generation) << 32) |
         static_cast<uint64_t>(static_cast<uint32_t>(slot + 1));
}

uint64_t make_control_event_id(int slot) {
  return kControlEventBase + static_cast<uint64_t>(slot);
}

int control_slot_from_event_id(uint64_t event_id) {
  if (event_id < kControlEventBase ||
      event_id >= kControlEventBase + static_cast<uint64_t>(kMaxControlConnections)) {
    return -1;
  }
  return static_cast<int>(event_id - kControlEventBase);
}

void release_connection(int epfd, int slot, std::vector<Connection>& conns,
                        std::vector<int>& free_slots) {
  (void)epfd;
  if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) return;
  Connection& conn = conns[static_cast<size_t>(slot)];
  int fd = conn.fd;
  if (fd < 0) return;
  close_fd(fd);
  clear_connection(conn);
  if (!g_fd_direct_slots) {
    free_slots.push_back(slot);
  }
}

const std::string_view& static_response(int code, bool keep_alive) {
  switch (code) {
    case 200:
      return keep_alive ? kReadyKeepAlive : kReadyClose;
    case 404:
      return keep_alive ? k404KeepAlive : k404Close;
    case 405:
      return keep_alive ? k405KeepAlive : k405Close;
    default:
      return k400Close;
  }
}

int clamp_fraud(int n) {
  if (n < 0) return 0;
  if (n > 5) return 5;
  return n;
}

void clear_output(Connection& conn) {
  conn.out_used = 0;
  conn.out_sent = 0;
  conn.out_static = nullptr;
}

bool queue_static_response(Connection& conn, std::string_view resp) {
  auto& outbuf = conn.buffers->outbuf;
  if (UNLIKELY(resp.size() > outbuf.size())) return false;
  if (conn.out_sent == conn.out_used) {
    if (g_direct_static_response) {
      conn.out_static = resp.data();
      conn.out_used = resp.size();
      conn.out_sent = 0;
      return true;
    }
    std::memcpy(outbuf.data(), resp.data(), resp.size());
    conn.out_used = resp.size();
    conn.out_sent = 0;
    return true;
  }
  if (conn.out_static) {
    const size_t remaining = conn.out_used - conn.out_sent;
    if (UNLIKELY(remaining > outbuf.size())) return false;
    std::memcpy(outbuf.data(), conn.out_static + conn.out_sent, remaining);
    conn.out_static = nullptr;
    conn.out_used = remaining;
    conn.out_sent = 0;
  }
  if (UNLIKELY(outbuf.size() - conn.out_used < resp.size())) return false;
  std::memcpy(outbuf.data() + conn.out_used, resp.data(), resp.size());
  conn.out_used += resp.size();
  return true;
}

bool env_equals(const char* value, const char* expected) {
  return value && std::strcmp(value, expected) == 0;
}

bool env_bool(const char* name, bool fallback) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  return value[0] == '1' || value[0] == 'y' || value[0] == 'Y' ||
         value[0] == 't' || value[0] == 'T';
}

int env_int(const char* name, int fallback, int min_value, int max_value) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (end == value || parsed < min_value || parsed > max_value) return fallback;
  return static_cast<int>(parsed);
}

bool env_int_present(const char* name, int& out, int min_value, int max_value) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return false;
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (end == value || parsed < min_value || parsed > max_value) return false;
  out = static_cast<int>(parsed);
  return true;
}

void set_socket_buffers(int fd, int rcvbuf, int sndbuf) {
  if (rcvbuf > 0) {
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  }
  if (sndbuf > 0) {
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
  }
}

void set_tcp_socket_tuning(int fd) {
  if (g_tcp_socket_prefer_busy_poll) {
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_PREFER_BUSY_POLL, &one, sizeof(one));
  }
  if (g_tcp_socket_busy_poll_us > 0) {
    int usecs = g_tcp_socket_busy_poll_us;
    setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &usecs, sizeof(usecs));
  }
  if (g_tcp_socket_busy_poll_budget > 0) {
    int budget = g_tcp_socket_busy_poll_budget;
    setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL_BUDGET, &budget, sizeof(budget));
  }
  if (g_tcp_notsent_lowat > 0) {
    int lowat = g_tcp_notsent_lowat;
    setsockopt(fd, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &lowat, sizeof(lowat));
  }
}

void handle_stop(int) {
  g_stop = 1;
}

uint64_t now_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

void cpu_relax();

void apply_scheduler_tuning() {
  if (g_set_nice_value) {
    if (setpriority(PRIO_PROCESS, 0, g_nice_value) != 0) {
      std::fprintf(stderr, "native setpriority(%d) failed: %s\n",
                   g_nice_value, std::strerror(errno));
    } else {
      errno = 0;
      int current = getpriority(PRIO_PROCESS, 0);
      if (errno == 0) {
        std::fprintf(stderr, "native nice value set to %d\n", current);
      }
    }
  }

  if (g_sched_fifo_priority > 0) {
    sched_param param{};
    param.sched_priority = g_sched_fifo_priority;
    if (sched_setscheduler(0, SCHED_FIFO, &param) != 0) {
      std::fprintf(stderr, "native sched_setscheduler(SCHED_FIFO,%d) failed: %s\n",
                   g_sched_fifo_priority, std::strerror(errno));
    } else {
      std::fprintf(stderr, "native SCHED_FIFO priority=%d enabled\n",
                   g_sched_fifo_priority);
    }
  }
}

int epoll_wait_idle(int epfd, epoll_event* events, int max_events, int idle_us) {
  if (idle_us <= 0) {
    return epoll_wait(epfd, events, max_events, -1);
  }

#ifdef SYS_epoll_pwait2
  timespec timeout{};
  timeout.tv_sec = idle_us / 1000000;
  timeout.tv_nsec = static_cast<long>(idle_us % 1000000) * 1000L;
  int n = static_cast<int>(
      syscall(SYS_epoll_pwait2, epfd, events, max_events, &timeout, nullptr, 0));
  if (n >= 0 || (errno != ENOSYS && errno != EINVAL)) {
    return n;
  }
#endif

  const int timeout_ms = (idle_us + 999) / 1000;
  return epoll_wait(epfd, events, max_events, timeout_ms > 0 ? timeout_ms : 1);
}

int epoll_wait_spin_then_block(int epfd, epoll_event* events, int max_events, int idle_us) {
  if (g_epoll_spin_us <= 0) {
    return epoll_wait_idle(epfd, events, max_events, idle_us);
  }

  const uint64_t deadline = now_ns() + static_cast<uint64_t>(g_epoll_spin_us) * 1000ull;
  for (;;) {
    int n = epoll_wait(epfd, events, max_events, 0);
    if (n != 0) return n;
    if (now_ns() >= deadline) break;
    cpu_relax();
  }
  return epoll_wait_idle(epfd, events, max_events, idle_us);
}

uint64_t avg_ns(uint64_t total, uint64_t count) {
  return count == 0 ? 0 : total / count;
}

void record_profile_ns(uint64_t ns, std::array<uint64_t, kProfileBucketCount>& bins,
                       uint64_t& max_ns) {
  if (ns > max_ns) max_ns = ns;
  for (size_t i = 0; i < kProfileBucketCount; ++i) {
    if (ns < kProfileBucketLimitsNs[i]) {
      ++bins[i];
      return;
    }
  }
}

void print_profile_bins(const char* prefix,
                        const std::array<uint64_t, kProfileBucketCount>& bins) {
  std::fprintf(stderr, "%s", prefix);
  for (size_t i = 0; i < kProfileBucketCount; ++i) {
    std::fprintf(stderr, " %.*s=%llu",
                 static_cast<int>(kProfileBucketLabels[i].size()),
                 kProfileBucketLabels[i].data(),
                 static_cast<unsigned long long>(bins[i]));
  }
  std::fprintf(stderr, "\n");
}

void record_tail_ns(uint64_t ns, std::array<uint64_t, kTailProfileBucketCount>& bins,
                    uint64_t& max_ns) {
  if (ns > max_ns) max_ns = ns;
  for (size_t i = 0; i < kTailProfileBucketCount; ++i) {
    if (ns < kTailProfileBucketLimitsNs[i]) {
      ++bins[i];
      return;
    }
  }
}

void print_tail_bins(const char* prefix,
                     const std::array<uint64_t, kTailProfileBucketCount>& bins) {
  std::fprintf(stderr, "%s", prefix);
  for (size_t i = 0; i < kTailProfileBucketCount; ++i) {
    std::fprintf(stderr, " %.*s=%llu",
                 static_cast<int>(kTailProfileBucketLabels[i].size()),
                 kTailProfileBucketLabels[i].data(),
                 static_cast<unsigned long long>(bins[i]));
  }
  std::fprintf(stderr, "\n");
}

void print_profile() {
  if (!g_profile || g_prof.requests == 0) return;
  if (g_cpp_ivf) {
    g_native_ivf.profile_report();
  }
  if (!g_fake_classifier && !g_cpp_ivf) {
    fraud_profile_report();
  }
  const uint64_t n = g_prof.requests;
  std::fprintf(stderr,
               "[native-profile] requests=%llu avg_body_bytes=%llu avg_classify_ns=%llu "
               "avg_vector_ns=%llu avg_bridge_ns=%llu avg_go_body_ns=%llu avg_fake_ns=%llu "
               "vector_failures=%llu total_classify_ms=%.3f total_vector_ms=%.3f "
               "total_bridge_ms=%.3f max_classify_ns=%llu max_vector_ns=%llu "
               "max_bridge_ns=%llu fast_post=%llu fallback_post=%llu ready=%llu "
               "bad=%llu other=%llu native_fast_hits=%llu native_fast_legit=%llu "
               "native_fast_fraud=%llu native_tier_hits=%llu direct_complete_post=%llu "
               "native_fast_score0=%llu native_fast_score1=%llu native_fast_score2=%llu "
               "native_fast_score3=%llu native_fast_score4=%llu native_fast_score5=%llu\n",
               static_cast<unsigned long long>(n),
               static_cast<unsigned long long>(avg_ns(g_prof.body_bytes, n)),
               static_cast<unsigned long long>(avg_ns(g_prof.classify_ns, n)),
               static_cast<unsigned long long>(avg_ns(g_prof.vector_ns, n)),
               static_cast<unsigned long long>(avg_ns(g_prof.bridge_ns, n)),
               static_cast<unsigned long long>(avg_ns(g_prof.go_body_ns, n)),
               static_cast<unsigned long long>(avg_ns(g_prof.fake_ns, n)),
               static_cast<unsigned long long>(g_prof.vector_failures),
               static_cast<double>(g_prof.classify_ns) / 1000000.0,
               static_cast<double>(g_prof.vector_ns) / 1000000.0,
               static_cast<double>(g_prof.bridge_ns) / 1000000.0,
               static_cast<unsigned long long>(g_prof.max_classify_ns),
               static_cast<unsigned long long>(g_prof.max_vector_ns),
               static_cast<unsigned long long>(g_prof.max_bridge_ns),
               static_cast<unsigned long long>(g_prof.fast_post_requests),
               static_cast<unsigned long long>(g_prof.fallback_post_requests),
               static_cast<unsigned long long>(g_prof.ready_requests),
               static_cast<unsigned long long>(g_prof.bad_requests),
               static_cast<unsigned long long>(g_prof.other_requests),
               static_cast<unsigned long long>(g_prof.native_fast_path_hits),
               static_cast<unsigned long long>(g_prof.native_fast_path_legit),
               static_cast<unsigned long long>(g_prof.native_fast_path_fraud),
               static_cast<unsigned long long>(g_prof.native_tier_path_hits),
               static_cast<unsigned long long>(g_prof.direct_complete_post_hits),
               static_cast<unsigned long long>(g_prof.native_fast_path_scores[0]),
               static_cast<unsigned long long>(g_prof.native_fast_path_scores[1]),
               static_cast<unsigned long long>(g_prof.native_fast_path_scores[2]),
               static_cast<unsigned long long>(g_prof.native_fast_path_scores[3]),
               static_cast<unsigned long long>(g_prof.native_fast_path_scores[4]),
               static_cast<unsigned long long>(g_prof.native_fast_path_scores[5]));
  print_profile_bins("[native-profile] classify_bins", g_prof.classify_bins);
  print_profile_bins("[native-profile] vector_bins", g_prof.vector_bins);
  print_profile_bins("[native-profile] bridge_bins", g_prof.bridge_bins);
}

void print_tail_profile() {
  if (!g_tail_profile) return;
  std::fprintf(stderr,
               "[native-tail-profile] parse_loops=%llu avg_parse_loop_ns=%llu "
               "max_parse_loop_ns=%llu flush_calls=%llu avg_flush_ns=%llu "
               "max_flush_ns=%llu client_events=%llu avg_client_event_ns=%llu "
               "max_client_event_ns=%llu\n",
               static_cast<unsigned long long>(g_tail.parse_loops),
               static_cast<unsigned long long>(avg_ns(g_tail.parse_loop_ns, g_tail.parse_loops)),
               static_cast<unsigned long long>(g_tail.parse_loop_max_ns),
               static_cast<unsigned long long>(g_tail.flush_calls),
               static_cast<unsigned long long>(avg_ns(g_tail.flush_ns, g_tail.flush_calls)),
               static_cast<unsigned long long>(g_tail.flush_max_ns),
               static_cast<unsigned long long>(g_tail.client_events),
               static_cast<unsigned long long>(avg_ns(g_tail.client_event_ns,
                                                       g_tail.client_events)),
               static_cast<unsigned long long>(g_tail.client_event_max_ns));
  print_tail_bins("[native-tail-profile] parse_loop_bins", g_tail.parse_loop_bins);
  print_tail_bins("[native-tail-profile] flush_bins", g_tail.flush_bins);
  print_tail_bins("[native-tail-profile] client_event_bins", g_tail.client_event_bins);
}

template <size_t N>
void record_bucket(uint64_t value, const std::array<uint64_t, N>& limits,
                   std::array<uint64_t, N>& bins) {
  for (size_t i = 0; i < N; ++i) {
    if (value <= limits[i]) {
      ++bins[i];
      return;
    }
  }
}

template <size_t N>
void print_named_bins(const char* prefix, const std::array<std::string_view, N>& labels,
                      const std::array<uint64_t, N>& bins) {
  std::fprintf(stderr, "%s", prefix);
  for (size_t i = 0; i < N; ++i) {
    std::fprintf(stderr, " %.*s=%llu",
                 static_cast<int>(labels[i].size()), labels[i].data(),
                 static_cast<unsigned long long>(bins[i]));
  }
  std::fprintf(stderr, "\n");
}

void record_epoll_batch(int n) {
  if (!g_event_profile) return;
  ++g_event_prof.epoll_waits;
  if (n == 0) {
    ++g_event_prof.epoll_timeouts;
    return;
  }
  if (n > 0) {
    ++g_event_prof.epoll_wakeups;
    g_event_prof.epoll_events += static_cast<uint64_t>(n);
    if (static_cast<uint64_t>(n) > g_event_prof.epoll_max_batch) {
      g_event_prof.epoll_max_batch = static_cast<uint64_t>(n);
    }
    record_bucket(static_cast<uint64_t>(n), kEpollBatchBucketLimits,
                  g_event_prof.epoll_batch_bins);
  }
}

void record_first_request(Connection& conn) {
  if (!g_event_profile || conn.first_request_recorded) return;
  conn.first_request_recorded = true;
  if (conn.accepted_ns == 0) return;
  const uint64_t wait_ns = now_ns() - conn.accepted_ns;
  ++g_event_prof.first_request_count;
  g_event_prof.first_request_wait_ns += wait_ns;
  if (wait_ns > g_event_prof.first_request_wait_max_ns) {
    g_event_prof.first_request_wait_max_ns = wait_ns;
  }
  record_bucket(wait_ns, kEventProfileBucketLimitsNs,
                g_event_prof.first_request_wait_bins);
}

void print_event_profile() {
  if (!g_event_profile) return;
  const uint64_t first_count = g_event_prof.first_request_count;
  const uint64_t avg_first =
      first_count == 0 ? 0 : g_event_prof.first_request_wait_ns / first_count;
  const uint64_t avg_batch =
      g_event_prof.epoll_wakeups == 0 ? 0 : g_event_prof.epoll_events / g_event_prof.epoll_wakeups;
  std::fprintf(stderr,
               "[native-event-profile] epoll_waits=%llu wakeups=%llu timeouts=%llu "
	       "short_waits=%llu long_waits=%llu events=%llu avg_batch=%llu "
	       "max_batch=%llu listen_events=%llu "
	       "control_events=%llu control_recv_calls=%llu control_got=%llu "
	       "control_empty=%llu control_errors=%llu control_loop_iters=%llu "
	       "control_loop_max_iters=%llu control_loop_over5=%llu "
	       "client_events=%llu fd_clients=%llu "
	       "initial_clients=%llu initial_data=%llu initial_empty=%llu "
               "initial_closed=%llu initial_bad=%llu post_flush_attempts=%llu "
               "post_flush_data=%llu post_flush_empty=%llu post_flush_closed=%llu "
               "post_flush_bad=%llu post_flush_responses=%llu epoll_mod_calls=%llu "
               "epoll_mod_skipped=%llu epoll_mod_errors=%llu first_requests=%llu "
               "avg_first_request_wait_ns=%llu max_first_request_wait_ns=%llu\n",
               static_cast<unsigned long long>(g_event_prof.epoll_waits),
               static_cast<unsigned long long>(g_event_prof.epoll_wakeups),
               static_cast<unsigned long long>(g_event_prof.epoll_timeouts),
               static_cast<unsigned long long>(g_event_prof.epoll_short_waits),
               static_cast<unsigned long long>(g_event_prof.epoll_long_waits),
               static_cast<unsigned long long>(g_event_prof.epoll_events),
               static_cast<unsigned long long>(avg_batch),
	       static_cast<unsigned long long>(g_event_prof.epoll_max_batch),
	       static_cast<unsigned long long>(g_event_prof.listen_events),
	       static_cast<unsigned long long>(g_event_prof.control_events),
	       static_cast<unsigned long long>(g_event_prof.control_recv_calls),
	       static_cast<unsigned long long>(g_event_prof.control_got),
	       static_cast<unsigned long long>(g_event_prof.control_empty),
	       static_cast<unsigned long long>(g_event_prof.control_errors),
	       static_cast<unsigned long long>(g_event_prof.control_loop_iters),
	       static_cast<unsigned long long>(g_event_prof.control_loop_max_iters),
	       static_cast<unsigned long long>(g_event_prof.control_loop_over5),
	       static_cast<unsigned long long>(g_event_prof.client_events),
               static_cast<unsigned long long>(g_event_prof.fd_clients),
               static_cast<unsigned long long>(g_event_prof.initial_read_clients),
               static_cast<unsigned long long>(g_event_prof.initial_read_data),
               static_cast<unsigned long long>(g_event_prof.initial_read_empty),
               static_cast<unsigned long long>(g_event_prof.initial_read_closed),
               static_cast<unsigned long long>(g_event_prof.initial_read_bad),
               static_cast<unsigned long long>(g_event_prof.post_flush_attempts),
               static_cast<unsigned long long>(g_event_prof.post_flush_data),
               static_cast<unsigned long long>(g_event_prof.post_flush_empty),
               static_cast<unsigned long long>(g_event_prof.post_flush_closed),
               static_cast<unsigned long long>(g_event_prof.post_flush_bad),
               static_cast<unsigned long long>(g_event_prof.post_flush_responses),
               static_cast<unsigned long long>(g_event_prof.epoll_mod_calls),
               static_cast<unsigned long long>(g_event_prof.epoll_mod_skipped),
               static_cast<unsigned long long>(g_event_prof.epoll_mod_errors),
               static_cast<unsigned long long>(first_count),
               static_cast<unsigned long long>(avg_first),
               static_cast<unsigned long long>(g_event_prof.first_request_wait_max_ns));
  print_named_bins("[native-event-profile] first_request_wait_bins",
                   kEventProfileBucketLabels, g_event_prof.first_request_wait_bins);
  print_named_bins("[native-event-profile] epoll_batch_bins",
                   kEpollBatchBucketLabels, g_event_prof.epoll_batch_bins);
}

void record_partial_bytes(size_t bytes) {
  if (!g_fragment_profile) return;
  if (bytes > g_frag.max_partial_bytes) g_frag.max_partial_bytes = static_cast<uint64_t>(bytes);
  if (bytes < 128) {
    ++g_frag.partial_lt_128;
  } else if (bytes < 512) {
    ++g_frag.partial_lt_512;
  } else if (bytes < 1024) {
    ++g_frag.partial_lt_1024;
  } else {
    ++g_frag.partial_ge_1024;
  }
}

void print_fragment_profile() {
  if (!g_fragment_profile) return;
  std::fprintf(stderr,
               "[native-frag] reads=%llu data=%llu empty=%llu closed=%llu full=%llu "
               "bytes=%llu avg_read_bytes=%llu drain_calls=%llu drain_data_calls=%llu "
               "max_reads_per_drain=%llu parse_waits=%llu fast_header_incomplete=%llu "
               "fallback_header_incomplete=%llu body_incomplete=%llu partial_lt128=%llu "
               "partial_lt512=%llu partial_lt1024=%llu partial_ge1024=%llu max_partial_bytes=%llu\n",
               static_cast<unsigned long long>(g_frag.read_calls),
               static_cast<unsigned long long>(g_frag.read_data),
               static_cast<unsigned long long>(g_frag.read_empty),
               static_cast<unsigned long long>(g_frag.read_closed),
               static_cast<unsigned long long>(g_frag.read_full),
               static_cast<unsigned long long>(g_frag.read_bytes),
               static_cast<unsigned long long>(avg_ns(g_frag.read_bytes, g_frag.read_data)),
               static_cast<unsigned long long>(g_frag.drain_calls),
               static_cast<unsigned long long>(g_frag.drain_data_calls),
               static_cast<unsigned long long>(g_frag.max_reads_per_drain),
               static_cast<unsigned long long>(g_frag.parse_waits),
               static_cast<unsigned long long>(g_frag.fast_header_incomplete),
               static_cast<unsigned long long>(g_frag.fallback_header_incomplete),
               static_cast<unsigned long long>(g_frag.body_incomplete),
               static_cast<unsigned long long>(g_frag.partial_lt_128),
               static_cast<unsigned long long>(g_frag.partial_lt_512),
               static_cast<unsigned long long>(g_frag.partial_lt_1024),
               static_cast<unsigned long long>(g_frag.partial_ge_1024),
               static_cast<unsigned long long>(g_frag.max_partial_bytes));
}

int fake_classify(const uint8_t* body, size_t n) {
  if (body == nullptr || n == 0) return -1;
  uint32_t x = static_cast<uint32_t>(n);
  x += static_cast<uint32_t>(body[0]);
  x += static_cast<uint32_t>(body[n >> 1]) << 1;
  x += static_cast<uint32_t>(body[n - 1]) << 2;
  return static_cast<int>(x % kFraudKeepAlive.size());
}

std::string_view find_request_id(const uint8_t* body, size_t n) {
  constexpr std::string_view needle = "\"id\":\"";
  if (!body || n < needle.size()) return {};
  const char* data = reinterpret_cast<const char*>(body);
  for (size_t i = 0; i + needle.size() <= n; ++i) {
    if (std::memcmp(data + i, needle.data(), needle.size()) != 0) continue;
    const size_t start = i + needle.size();
    size_t end = start;
    while (end < n && data[end] != '"') ++end;
    return std::string_view(data + start, end - start);
  }
  return {};
}

int classify_body(const uint8_t* body, size_t n) {
  if (!g_profile) {
    if (g_fake_classifier) return fake_classify(body, n);
    if (g_native_tier_path) {
      std::array<float, 16> vector;
      int tier_score = -1;
      if (!build_fraud_vector_cpp_with_tier_path(body, n, vector.data(),
                                                 g_native_fast_path_mode, true,
                                                 &tier_score)) {
        return -1;
      }
      return tier_score;
    }
    if (g_cpp_ivf) {
      std::array<float, 16> vector;
      int fast_score = -1;
      int route_score = -1;
      if (g_native_fast_path) {
        if (g_native_fast_fraud_ivf_route) {
          const int direct_mode =
              g_native_fast_path_mode & ~(kNativeFastPathFraud | kNativeFastPathExtremeFraud);
          if (!build_fraud_vector_cpp_with_fast_route(
                  body, n, vector.data(), direct_mode, kNativeFastPathFraud,
                  &fast_score, &route_score)) {
            return -1;
          }
        } else if (!build_fraud_vector_cpp_with_fast_path(body, n, vector.data(),
                                                          g_native_fast_path_mode, &fast_score)) {
          return -1;
        }
        if (fast_score >= 0) return fast_score;
      } else if (!build_fraud_vector_cpp(body, n, vector.data())) {
        return -1;
      }
      if (g_ivf_trace) {
        std::string_view id = find_request_id(body, n);
        g_native_ivf.set_trace_label(id.data(), id.size());
      }
      if (route_score == 5 && g_native_fast_fraud_ivf_quick_probe > 0) {
        return g_native_ivf.classify_with_quick_probe(
            vector.data(), g_native_fast_fraud_ivf_quick_probe);
      }
      return g_native_ivf.classify(vector.data());
    }
    if (g_cpp_vector_parser) {
      std::array<float, 16> vector;
      if (!build_fraud_vector_cpp(body, n, vector.data())) return -1;
      return fraud_classify_vector(vector.data());
    }
    return fraud_classify(const_cast<uint8_t*>(body), n);
  }

  const uint64_t t0 = now_ns();
  ++g_prof.requests;
  g_prof.body_bytes += static_cast<uint64_t>(n);
  if (g_fake_classifier) {
    int out = fake_classify(body, n);
    const uint64_t t1 = now_ns();
    const uint64_t total_ns = t1 - t0;
    g_prof.fake_ns += total_ns;
    g_prof.classify_ns += total_ns;
    record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
    return out;
  }
  if (g_native_tier_path) {
    std::array<float, 16> vector;
    const uint64_t tv0 = now_ns();
    int tier_score = -1;
    if (!build_fraud_vector_cpp_with_tier_path(body, n, vector.data(),
                                               g_native_fast_path_mode, true,
                                               &tier_score)) {
      const uint64_t tv1 = now_ns();
      const uint64_t vector_ns = tv1 - tv0;
      const uint64_t total_ns = tv1 - t0;
      ++g_prof.vector_failures;
      g_prof.vector_ns += vector_ns;
      g_prof.classify_ns += total_ns;
      record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
      record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
      return -1;
    }
    const uint64_t tv1 = now_ns();
    const uint64_t vector_ns = tv1 - tv0;
    const uint64_t total_ns = tv1 - t0;
    g_prof.vector_ns += vector_ns;
    g_prof.classify_ns += total_ns;
    ++g_prof.native_tier_path_hits;
    record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
    record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
    return tier_score;
  }
  if (g_cpp_ivf) {
    std::array<float, 16> vector;
    const uint64_t tv0 = now_ns();
    int fast_score = -1;
    int route_score = -1;
    bool ok = false;
    if (g_native_fast_path) {
      if (g_native_fast_fraud_ivf_route) {
        const int direct_mode =
            g_native_fast_path_mode & ~(kNativeFastPathFraud | kNativeFastPathExtremeFraud);
        ok = build_fraud_vector_cpp_with_fast_route(
            body, n, vector.data(), direct_mode, kNativeFastPathFraud,
            &fast_score, &route_score);
      } else {
        ok = build_fraud_vector_cpp_with_fast_path(body, n, vector.data(),
                                                   g_native_fast_path_mode, &fast_score);
      }
    } else {
      ok = build_fraud_vector_cpp(body, n, vector.data());
    }
    if (!ok) {
      const uint64_t tv1 = now_ns();
      const uint64_t vector_ns = tv1 - tv0;
      const uint64_t total_ns = tv1 - t0;
      ++g_prof.vector_failures;
      g_prof.vector_ns += vector_ns;
      g_prof.classify_ns += total_ns;
      record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
      record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
      return -1;
    }
    const uint64_t tv1 = now_ns();
    if (fast_score >= 0) {
      const uint64_t vector_ns = tv1 - tv0;
      const uint64_t total_ns = tv1 - t0;
      g_prof.vector_ns += vector_ns;
      g_prof.classify_ns += total_ns;
      ++g_prof.native_fast_path_hits;
      if (fast_score == 0) ++g_prof.native_fast_path_legit;
      if (fast_score == 5) ++g_prof.native_fast_path_fraud;
      if (fast_score >= 0 && fast_score < static_cast<int>(g_prof.native_fast_path_scores.size())) {
        ++g_prof.native_fast_path_scores[static_cast<size_t>(fast_score)];
      }
      record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
      record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
      return fast_score;
    }
    if (g_ivf_trace) {
      std::string_view id = find_request_id(body, n);
      g_native_ivf.set_trace_label(id.data(), id.size());
    }
    int out = (route_score == 5 && g_native_fast_fraud_ivf_quick_probe > 0)
                  ? g_native_ivf.classify_with_quick_probe(
                        vector.data(), g_native_fast_fraud_ivf_quick_probe)
                  : g_native_ivf.classify(vector.data());
    const uint64_t tb1 = now_ns();
    const uint64_t vector_ns = tv1 - tv0;
    const uint64_t bridge_ns = tb1 - tv1;
    const uint64_t total_ns = tb1 - t0;
    g_prof.vector_ns += vector_ns;
    g_prof.bridge_ns += bridge_ns;
    g_prof.classify_ns += total_ns;
    record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
    record_profile_ns(bridge_ns, g_prof.bridge_bins, g_prof.max_bridge_ns);
    record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
    return out;
  }
  if (g_cpp_vector_parser) {
    std::array<float, 16> vector;
    const uint64_t tv0 = now_ns();
    if (!build_fraud_vector_cpp(body, n, vector.data())) {
      const uint64_t tv1 = now_ns();
      const uint64_t vector_ns = tv1 - tv0;
      const uint64_t total_ns = tv1 - t0;
      ++g_prof.vector_failures;
      g_prof.vector_ns += vector_ns;
      g_prof.classify_ns += total_ns;
      record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
      record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
      return -1;
    }
    const uint64_t tv1 = now_ns();
    int out = fraud_classify_vector(vector.data());
    const uint64_t tb1 = now_ns();
    const uint64_t vector_ns = tv1 - tv0;
    const uint64_t bridge_ns = tb1 - tv1;
    const uint64_t total_ns = tb1 - t0;
    g_prof.vector_ns += vector_ns;
    g_prof.bridge_ns += bridge_ns;
    g_prof.classify_ns += total_ns;
    record_profile_ns(vector_ns, g_prof.vector_bins, g_prof.max_vector_ns);
    record_profile_ns(bridge_ns, g_prof.bridge_bins, g_prof.max_bridge_ns);
    record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
    return out;
  }
  int out = fraud_classify(const_cast<uint8_t*>(body), n);
  const uint64_t t1 = now_ns();
  const uint64_t total_ns = t1 - t0;
  g_prof.go_body_ns += total_ns;
  g_prof.classify_ns += total_ns;
  record_profile_ns(total_ns, g_prof.classify_bins, g_prof.max_classify_ns);
  return out;
}

void run_startup_warmup() {
  if (g_startup_warmup_requests <= 0) return;

  const bool saved_profile = g_profile;
  g_profile = false;
  int checksum = 0;
  const uint64_t start = now_ns();
  for (int i = 0; i < g_startup_warmup_requests; ++i) {
    const std::string_view body =
        kStartupWarmupPayloads[static_cast<size_t>(i) % kStartupWarmupPayloads.size()];
    checksum += classify_body(reinterpret_cast<const uint8_t*>(body.data()), body.size());
  }
  const uint64_t end = now_ns();
  g_profile = saved_profile;

  std::fprintf(stderr, "native startup warmup: requests=%d checksum=%d elapsed=%.3fms\n",
               g_startup_warmup_requests, checksum,
               static_cast<double>(end - start) / 1000000.0);
}

int parse_tcp_port(const char* addr_env) {
  std::string addr = addr_env ? addr_env : "0.0.0.0:8081";
  auto pos = addr.rfind(':');
  if (pos == std::string::npos) return 8081;
  int port = std::atoi(addr.substr(pos + 1).c_str());
  return port > 0 ? port : 8081;
}

void send_all_blocking(int fd, const char* data, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t n = send(fd, data + off, len - off, MSG_NOSIGNAL);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    break;
  }
}

void run_tcp_self_warm_client(int port, int requests, int delay_ms) {
  if (delay_ms > 0) {
    timespec ts{};
    ts.tv_sec = delay_ms / 1000;
    ts.tv_nsec = static_cast<long>(delay_ms % 1000) * 1000000L;
    nanosleep(&ts, nullptr);
  }

  std::array<char, 1024> req{};
  std::array<char, 512> resp{};
  for (int i = 0; i < requests; ++i) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) continue;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
      close_fd(fd);
      continue;
    }

    std::string_view body =
        kStartupWarmupPayloads[static_cast<size_t>(i) % kStartupWarmupPayloads.size()];
    int n = std::snprintf(req.data(), req.size(),
                          "POST /fraud-score HTTP/1.1\r\n"
                          "Host: 127.0.0.1\r\n"
                          "User-Agent: self-warm\r\n"
                          "Content-Length: %zu\r\n"
                          "Content-Type: application/json\r\n\r\n",
                          body.size());
    if (n > 0 && static_cast<size_t>(n) < req.size()) {
      send_all_blocking(fd, req.data(), static_cast<size_t>(n));
      send_all_blocking(fd, body.data(), body.size());
      recv(fd, resp.data(), resp.size(), 0);
    }
    close_fd(fd);
  }
}

void poll_tcp_self_warm_child() {
  if (g_tcp_self_warm_pid <= 0 || g_tcp_self_warm_complete) return;
  int status = 0;
  pid_t done = waitpid(g_tcp_self_warm_pid, &status, WNOHANG);
  if (done == g_tcp_self_warm_pid) {
    g_tcp_self_warm_complete = true;
    g_tcp_self_warm_pid = -1;
    std::fprintf(stderr, "native TCP self-warm complete status=%d\n", status);
  }
}

void start_tcp_self_warm_child(const char* addr) {
  if (g_tcp_self_warm_requests <= 0) return;
  const int port = parse_tcp_port(addr);
  g_tcp_self_warm_complete = false;
  pid_t pid = fork();
  if (pid == 0) {
    run_tcp_self_warm_client(port, g_tcp_self_warm_requests, g_tcp_self_warm_delay_ms);
    _exit(0);
  }
  if (pid < 0) {
    std::perror("tcp self-warm fork");
    g_tcp_self_warm_complete = true;
    return;
  }
  g_tcp_self_warm_pid = pid;
  std::fprintf(stderr, "native TCP self-warm started pid=%d requests=%d delay_ms=%d\n",
               static_cast<int>(pid), g_tcp_self_warm_requests, g_tcp_self_warm_delay_ms);
}

size_t find_crlf(const char* data, size_t from, size_t limit) {
  for (size_t i = from; i + 1 < limit; ++i) {
    if (data[i] == '\r' && data[i + 1] == '\n') return i;
  }
  return std::string_view::npos;
}

size_t find_crlf_lf_memchr(const char* data, size_t from, size_t limit) {
  if (from >= limit) return std::string_view::npos;
  const char* cur = data + from;
  const char* end = data + limit;
  while (cur < end) {
    const void* found = std::memchr(cur, '\n', static_cast<size_t>(end - cur));
    if (found == nullptr) return std::string_view::npos;
    const char* lf = static_cast<const char*>(found);
    if (lf > data && lf[-1] == '\r') {
      return static_cast<size_t>((lf - 1) - data);
    }
    cur = lf + 1;
  }
  return std::string_view::npos;
}

size_t find_headers_end(const char* data, size_t len) {
  if (len < 4) return std::string_view::npos;
  const void* found = memmem(data, len, "\r\n\r\n", 4);
  if (found == nullptr) return std::string_view::npos;
  return static_cast<size_t>(static_cast<const char*>(found) - data);
}

char ascii_lower(char c) {
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

bool eq_icase_lit(const char* data, size_t len, std::string_view lit) {
  if (len != lit.size()) return false;
  for (size_t i = 0; i < len; ++i) {
    if (ascii_lower(data[i]) != ascii_lower(lit[i])) return false;
  }
  return true;
}

bool parse_content_length(const char* value, size_t len, size_t& out) {
  size_t i = 0;
  while (i < len && (value[i] == ' ' || value[i] == '\t')) ++i;
  if (i == len || value[i] < '0' || value[i] > '9') return false;
  size_t n = 0;
  for (; i < len && value[i] >= '0' && value[i] <= '9'; ++i) {
    n = (n * 10) + static_cast<size_t>(value[i] - '0');
  }
  while (i < len && (value[i] == ' ' || value[i] == '\t')) ++i;
  if (i != len) return false;
  out = n;
  return true;
}

bool fast_parse_content_length_line(const char* line, size_t line_len, size_t& out) {
  if (line_len <= 15 || line[14] != ':') return false;
  if (std::memcmp(line, "Content-Length", 14) != 0) return false;
  size_t i = 15;
  if (i < line_len && line[i] == ' ') ++i;
  if (i == line_len || line[i] < '0' || line[i] > '9') return false;
  size_t n = 0;
  for (; i < line_len && line[i] >= '0' && line[i] <= '9'; ++i) {
    n = (n * 10) + static_cast<size_t>(line[i] - '0');
  }
  if (i != line_len) return false;
  out = n;
  return true;
}

bool value_is_close(const char* value, size_t len) {
  while (len > 0 && (value[0] == ' ' || value[0] == '\t')) {
    ++value;
    --len;
  }
  while (len > 0 && (value[len - 1] == ' ' || value[len - 1] == '\t')) --len;
  return eq_icase_lit(value, len, "close");
}

bool line_eq(const char* data, size_t len, std::string_view lit) {
  return len == lit.size() && std::memcmp(data, lit.data(), len) == 0;
}

bool fixed_request_line(const char* data, size_t headers_end, std::string_view lit, size_t& req_line_end) {
  const size_t n = lit.size();
  if (headers_end < n + 2) return false;
  if (std::memcmp(data, lit.data(), n) != 0) return false;
  if (data[n] != '\r' || data[n + 1] != '\n') return false;
  req_line_end = n;
  return true;
}

bool fixed_request_prefix(const char* data, size_t len, std::string_view lit, size_t& req_line_end) {
  const size_t n = lit.size();
  if (UNLIKELY(len < n + 2)) return false;
  if (UNLIKELY(std::memcmp(data, lit.data(), n) != 0)) return false;
  if (UNLIKELY(data[n] != '\r' || data[n + 1] != '\n')) return false;
  req_line_end = n;
  return true;
}

size_t find_lit(const char* data, size_t from, size_t limit, std::string_view needle) {
  if (needle.empty() || limit < from || limit - from < needle.size()) return std::string_view::npos;
  const void* found = memmem(data + from, limit - from, needle.data(), needle.size());
  if (found == nullptr) return std::string_view::npos;
  return static_cast<size_t>(static_cast<const char*>(found) - data);
}

bool fast_content_length(const char* data, size_t from, size_t headers_end, size_t& out) {
  constexpr std::string_view first_key = "Content-Length:";
  if (from + first_key.size() <= headers_end && std::memcmp(data + from, first_key.data(), first_key.size()) == 0) {
    size_t value_start = from + first_key.size();
    size_t value_end = find_crlf_lf_memchr(data, value_start, headers_end + 2);
    if (value_end == std::string_view::npos || value_end > headers_end) return false;
    return parse_content_length(data + value_start, value_end - value_start, out);
  }

  constexpr std::string_view key = "\r\nContent-Length:";
  size_t key_pos = find_lit(data, from, headers_end + 2, key);
  if (key_pos == std::string_view::npos) return false;
  size_t value_start = key_pos + key.size();
  size_t value_end = find_crlf_lf_memchr(data, value_start, headers_end + 2);
  if (value_end == std::string_view::npos || value_end > headers_end) return false;
  return parse_content_length(data + value_start, value_end - value_start, out);
}

enum class FastHeaderStatus {
  Incomplete,
  Ready,
  Fallback,
};

FastHeaderStatus fast_k6_post_headers(const char* data, size_t len, size_t headers_start,
                                      size_t& headers_end, size_t& content_length) {
  constexpr std::string_view kHost = "Host: ";
  constexpr std::string_view kUserAgent = "User-Agent: ";
  constexpr std::string_view kContentLength = "Content-Length: ";
  constexpr std::string_view kContentType = "Content-Type: application/json";

  size_t p = headers_start;
  if (p + kHost.size() > len) return FastHeaderStatus::Incomplete;
  if (std::memcmp(data + p, kHost.data(), kHost.size()) != 0) return FastHeaderStatus::Fallback;
  p = find_crlf_lf_memchr(data, p + kHost.size(), len);
  if (p == std::string_view::npos) return FastHeaderStatus::Incomplete;
  p += 2;

  if (p + kUserAgent.size() > len) return FastHeaderStatus::Incomplete;
  if (std::memcmp(data + p, kUserAgent.data(), kUserAgent.size()) != 0) return FastHeaderStatus::Fallback;
  p = find_crlf_lf_memchr(data, p + kUserAgent.size(), len);
  if (p == std::string_view::npos) return FastHeaderStatus::Incomplete;
  p += 2;

  if (p + kContentLength.size() > len) return FastHeaderStatus::Incomplete;
  if (std::memcmp(data + p, kContentLength.data(), kContentLength.size()) != 0) {
    return FastHeaderStatus::Fallback;
  }
  p += kContentLength.size();
  if (p >= len) return FastHeaderStatus::Incomplete;
  if (data[p] < '0' || data[p] > '9') return FastHeaderStatus::Fallback;
  size_t n = 0;
  while (p < len && data[p] >= '0' && data[p] <= '9') {
    n = (n * 10) + static_cast<size_t>(data[p] - '0');
    ++p;
  }
  if (p + 2 > len) return FastHeaderStatus::Incomplete;
  if (data[p] != '\r' || data[p + 1] != '\n') return FastHeaderStatus::Fallback;
  p += 2;

  if (p + kContentType.size() + 4 > len) return FastHeaderStatus::Incomplete;
  if (std::memcmp(data + p, kContentType.data(), kContentType.size()) != 0) {
    return FastHeaderStatus::Fallback;
  }
  p += kContentType.size();
  if (data[p] != '\r' || data[p + 1] != '\n' || data[p + 2] != '\r' || data[p + 3] != '\n') {
    return FastHeaderStatus::Fallback;
  }

  headers_end = p;
  content_length = n;
  return FastHeaderStatus::Ready;
}

FastHeaderStatus fast_post_headers(const char* data, size_t len, size_t headers_start,
                                   size_t& headers_end, size_t& content_length) {
  bool found_content_length = false;
  size_t off = headers_start;
  while (off < len) {
    size_t line_end = find_crlf_lf_memchr(data, off, len);
    if (line_end == std::string_view::npos) return FastHeaderStatus::Incomplete;
    if (line_end == off) {
      if (off < 2) return FastHeaderStatus::Fallback;
      headers_end = off - 2;
      return found_content_length ? FastHeaderStatus::Ready : FastHeaderStatus::Fallback;
    }

    const char* line = data + off;
    const size_t line_len = line_end - off;
    if (line_len > 15 && line[14] == ':') {
      if (!fast_parse_content_length_line(line, line_len, content_length)) return FastHeaderStatus::Fallback;
      found_content_length = true;
    }
    off = line_end + 2;
  }
  return FastHeaderStatus::Incomplete;
}

bool parse_request(Connection& conn, size_t& consumed) {
  auto& inbuf = conn.buffers->inbuf;
  const char* data = inbuf.data() + conn.in_start;
  const size_t data_len = conn.in_end - conn.in_start;
  size_t req_line_end = 0;
  if (LIKELY(fixed_request_prefix(data, data_len, kPostFraudLine, req_line_end))) {
    size_t headers_end = 0;
    size_t content_length = 0;
    FastHeaderStatus status = fast_k6_post_headers(data, data_len, req_line_end + 2, headers_end, content_length);
    if (UNLIKELY(status == FastHeaderStatus::Fallback)) {
      status = fast_post_headers(data, data_len, req_line_end + 2, headers_end, content_length);
    }
    if (UNLIKELY(status == FastHeaderStatus::Incomplete)) {
      if (g_fragment_profile) ++g_frag.fast_header_incomplete;
      return false;
    }
    if (LIKELY(status == FastHeaderStatus::Ready)) {
      size_t body_start = headers_end + 4;
      size_t total_needed = body_start + content_length;
      if (UNLIKELY(content_length == 0 || content_length > static_cast<size_t>(kReadBufSize))) {
        if (g_profile) ++g_prof.bad_requests;
        clear_output(conn);
        if (!queue_static_response(conn, k400Close)) return false;
        conn.close_after_write = true;
        consumed = data_len;
        return true;
      }
      if (UNLIKELY(data_len < total_needed)) {
        if (g_fragment_profile) ++g_frag.body_incomplete;
        return false;
      }

      consumed = total_needed;
      conn.close_after_write = false;
      record_first_request(conn);
      if (g_profile) ++g_prof.fast_post_requests;
      auto* body = reinterpret_cast<uint8_t*>(inbuf.data() + conn.in_start + body_start);
      int fraud = classify_body(body, content_length);
      if (UNLIKELY(fraud < 0)) {
        if (g_profile) ++g_prof.bad_requests;
        clear_output(conn);
        if (!queue_static_response(conn, k400Close)) return false;
        conn.close_after_write = true;
        return true;
      }
      const auto& resp = kFraudKeepAlive[clamp_fraud(fraud)];
      return queue_static_response(conn, resp);
    }
  }

  size_t headers_end = find_headers_end(data, data_len);
  if (UNLIKELY(headers_end == std::string_view::npos)) {
    if (g_fragment_profile) ++g_frag.fallback_header_incomplete;
    return false;
  }

  req_line_end = 0;
  bool is_get_ready = false;
  bool is_post_fraud = fixed_request_line(data, headers_end, kPostFraudLine, req_line_end);
  if (!is_post_fraud) {
    is_get_ready = fixed_request_line(data, headers_end, kGetReadyLine, req_line_end);
  }
  if (!is_post_fraud && !is_get_ready) {
    req_line_end = find_crlf(data, 0, headers_end + 2);
    if (req_line_end == std::string_view::npos || req_line_end > headers_end) {
      if (g_profile) ++g_prof.bad_requests;
      clear_output(conn);
      if (!queue_static_response(conn, k400Close)) return false;
      conn.close_after_write = true;
      consumed = data_len;
      return true;
    }
    is_get_ready = line_eq(data, req_line_end, kGetReadyLine);
    is_post_fraud = line_eq(data, req_line_end, kPostFraudLine);
  }
  const bool is_any_get = is_get_ready || (req_line_end >= 4 && std::memcmp(data, "GET ", 4) == 0);
  bool keep_alive = is_post_fraud || is_get_ready;
  size_t content_length = 0;

  const size_t headers_start = req_line_end + 2;
  bool headers_parsed = false;
  if (is_post_fraud && fast_content_length(data, headers_start, headers_end, content_length)) {
    headers_parsed = true;
  }

  if (!headers_parsed) {
    size_t off = headers_start;
    while (off < headers_end) {
      size_t next = find_crlf(data, off, headers_end + 2);
      if (next == std::string_view::npos || next > headers_end) {
        if (g_profile) ++g_prof.bad_requests;
        clear_output(conn);
        if (!queue_static_response(conn, k400Close)) return false;
        conn.close_after_write = true;
        consumed = data_len;
        return true;
      }
      const char* line = data + off;
      const size_t line_len = next - off;
      size_t colon = 0;
      while (colon < line_len && line[colon] != ':') ++colon;
      if (colon < line_len) {
        const char* value = line + colon + 1;
        const size_t value_len = line_len - colon - 1;
        if (colon == 14 && eq_icase_lit(line, colon, "Content-Length")) {
          if (!parse_content_length(value, value_len, content_length)) {
            content_length = 0;
          }
        } else if (!is_post_fraud && colon == 10 && eq_icase_lit(line, colon, "Connection")) {
          if (value_is_close(value, value_len)) {
            keep_alive = false;
          }
        }
      }
      off = next + 2;
    }
  }

  size_t body_start = headers_end + 4;
  size_t total_needed = body_start;
  if (is_post_fraud) {
    total_needed += content_length;
    if (content_length == 0 || content_length > static_cast<size_t>(kReadBufSize)) {
      if (g_profile) ++g_prof.bad_requests;
      clear_output(conn);
      if (!queue_static_response(conn, k400Close)) return false;
      conn.close_after_write = true;
      consumed = data_len;
      return true;
    }
  }
  if (UNLIKELY(data_len < total_needed)) {
    if (g_fragment_profile) ++g_frag.body_incomplete;
    return false;
  }

  consumed = total_needed;
  conn.close_after_write = !keep_alive;
  record_first_request(conn);

  if (is_get_ready) {
    if (g_profile) ++g_prof.ready_requests;
    if (UNLIKELY(!g_tcp_self_warm_complete)) {
      conn.close_after_write = false;
      return queue_static_response(conn, kReadyUnavailable);
    }
    auto resp = static_response(200, keep_alive);
    return queue_static_response(conn, resp);
  }
  if (is_post_fraud) {
    if (g_profile) ++g_prof.fallback_post_requests;
    auto* body = reinterpret_cast<uint8_t*>(inbuf.data() + conn.in_start + body_start);
    int fraud = classify_body(body, content_length);
    if (UNLIKELY(fraud < 0)) {
      if (g_profile) ++g_prof.bad_requests;
      clear_output(conn);
      if (!queue_static_response(conn, k400Close)) return false;
      conn.close_after_write = true;
      return true;
    }
    const auto& resp = keep_alive ? kFraudKeepAlive[clamp_fraud(fraud)] : kFraudClose[clamp_fraud(fraud)];
    return queue_static_response(conn, resp);
  }

  if (g_profile) ++g_prof.other_requests;
  auto resp = static_response(is_any_get ? 404 : 405, keep_alive);
  return queue_static_response(conn, resp);
}

void compact_input(Connection& conn) {
  auto& inbuf = conn.buffers->inbuf;
  size_t remaining = conn.in_end - conn.in_start;
  const bool should_compact = conn.in_end == inbuf.size() ||
                              (g_aggressive_compact && conn.in_start > remaining);
  if (conn.in_start > 0 && should_compact) {
    if (remaining > 0) {
      std::memmove(inbuf.data(), inbuf.data() + conn.in_start, remaining);
    }
    conn.in_start = 0;
    conn.in_end = remaining;
  }
}

void queue_bad_request(Connection& conn) {
  clear_output(conn);
  queue_static_response(conn, k400Close);
  conn.close_after_write = true;
}

enum class InputStatus {
  Data,
  Empty,
  Closed,
  BadRequest,
};

InputStatus try_direct_complete_post(int fd, Connection& conn, size_t bytes_read) {
  if (!g_direct_complete_post) return InputStatus::Empty;
  if (UNLIKELY(conn.in_start != 0 || conn.in_end != 0 || conn.out_sent != conn.out_used)) {
    return InputStatus::Empty;
  }

  auto& inbuf = conn.buffers->inbuf;
  const char* data = inbuf.data();
  size_t req_line_end = 0;
  if (UNLIKELY(!fixed_request_prefix(data, bytes_read, kPostFraudLine, req_line_end))) {
    return InputStatus::Empty;
  }

  size_t headers_end = 0;
  size_t content_length = 0;
  FastHeaderStatus status = fast_k6_post_headers(data, bytes_read, req_line_end + 2,
                                                  headers_end, content_length);
  if (UNLIKELY(status == FastHeaderStatus::Fallback)) {
    status = fast_post_headers(data, bytes_read, req_line_end + 2, headers_end, content_length);
  }
  if (UNLIKELY(status != FastHeaderStatus::Ready)) {
    return InputStatus::Empty;
  }

  const size_t body_start = headers_end + 4;
  const size_t total_needed = body_start + content_length;
  if (UNLIKELY(content_length == 0 || content_length > static_cast<size_t>(kReadBufSize))) {
    return InputStatus::Empty;
  }
  if (UNLIKELY(bytes_read < total_needed)) {
    return InputStatus::Empty;
  }

  record_first_request(conn);
  auto* body = reinterpret_cast<uint8_t*>(inbuf.data() + body_start);
  int fraud = classify_body(body, content_length);
  if (UNLIKELY(fraud < 0)) {
    return InputStatus::Empty;
  }
  conn.in_start = total_needed;
  conn.in_end = bytes_read;
  if (conn.in_start == conn.in_end) {
    conn.in_start = 0;
    conn.in_end = 0;
  }

  const auto& resp = kFraudKeepAlive[clamp_fraud(fraud)];
  if (g_profile) ++g_prof.direct_complete_post_hits;
  const char* out = resp.data();
  const size_t remaining = resp.size();
  ssize_t w = g_rw_syscalls ? write(fd, out, remaining)
                             : send(fd, out, remaining, MSG_NOSIGNAL);
  if (UNLIKELY(w < 0)) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) return InputStatus::Closed;
    conn.out_static = out;
    conn.out_used = remaining;
    conn.out_sent = 0;
    conn.close_after_write = false;
    return InputStatus::Data;
  }
  if (UNLIKELY(w == 0)) {
    conn.out_static = out;
    conn.out_used = remaining;
    conn.out_sent = 0;
    conn.close_after_write = false;
    return InputStatus::Data;
  }
  if (UNLIKELY(static_cast<size_t>(w) < remaining)) {
    conn.out_static = out;
    conn.out_used = remaining;
    conn.out_sent = static_cast<size_t>(w);
    conn.close_after_write = false;
    return InputStatus::Data;
  }
  return InputStatus::Data;
}

InputStatus read_input_once(int fd, Connection& conn, bool* filled_read = nullptr) {
  if (filled_read) *filled_read = false;
  auto& inbuf = conn.buffers->inbuf;
  if (conn.in_end == inbuf.size()) {
    if (g_fragment_profile) ++g_frag.read_full;
    queue_bad_request(conn);
    return InputStatus::BadRequest;
  }

  if (g_fragment_profile) ++g_frag.read_calls;
  const size_t read_space = inbuf.size() - conn.in_end;
  ssize_t r = g_rw_syscalls ? read(fd, inbuf.data() + conn.in_end, read_space)
                             : recv(fd, inbuf.data() + conn.in_end, read_space, 0);
  if (UNLIKELY(r == 0)) {
    if (g_fragment_profile) ++g_frag.read_closed;
    return InputStatus::Closed;
  }
  if (UNLIKELY(r < 0)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      if (g_fragment_profile) ++g_frag.read_empty;
      return InputStatus::Empty;
    }
    return InputStatus::Closed;
  }
  if (LIKELY(r > 0)) {
    if (filled_read) *filled_read = static_cast<size_t>(r) == read_space;
    InputStatus direct = try_direct_complete_post(fd, conn, static_cast<size_t>(r));
    if (direct != InputStatus::Empty) {
      if (g_fragment_profile) {
        ++g_frag.read_data;
        g_frag.read_bytes += static_cast<uint64_t>(r);
      }
      return direct;
    }
  }
  conn.in_end += static_cast<size_t>(r);
  if (g_fragment_profile) {
    ++g_frag.read_data;
    g_frag.read_bytes += static_cast<uint64_t>(r);
  }
  return InputStatus::Data;
}

InputStatus drain_input(int fd, Connection& conn) {
  compact_input(conn);
  InputStatus last = InputStatus::Empty;
  uint64_t reads_this_drain = 0;
  if (g_fragment_profile) ++g_frag.drain_calls;
  for (;;) {
    bool filled_read = false;
    InputStatus status = read_input_once(fd, conn, &filled_read);
    if (status == InputStatus::Data) {
      last = InputStatus::Data;
      if (g_fragment_profile) ++reads_this_drain;
      if (g_single_recv_per_event && !filled_read) return last;
      continue;
    }
    if (g_fragment_profile && reads_this_drain > 0) {
      ++g_frag.drain_data_calls;
      if (reads_this_drain > g_frag.max_reads_per_drain) {
        g_frag.max_reads_per_drain = reads_this_drain;
      }
    }
    if (status == InputStatus::Empty) return last;
    return status;
  }
}

void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
  __asm__ __volatile__("pause" ::: "memory");
#endif
}

InputStatus spin_read_input(int fd, Connection& conn, int spins) {
  compact_input(conn);
  InputStatus last = InputStatus::Empty;
  uint64_t reads_this_drain = 0;
  if (g_fragment_profile) ++g_frag.drain_calls;
  for (int spin = 0; spin < spins; ++spin) {
    InputStatus status = read_input_once(fd, conn);
    if (status == InputStatus::Data) {
      last = InputStatus::Data;
      if (g_fragment_profile) ++reads_this_drain;
      continue;
    }
    if (g_fragment_profile && reads_this_drain > 0) {
      ++g_frag.drain_data_calls;
      if (reads_this_drain > g_frag.max_reads_per_drain) {
        g_frag.max_reads_per_drain = reads_this_drain;
      }
    }
    if (status == InputStatus::Empty) {
      if (last == InputStatus::Data) return last;
      cpu_relax();
      continue;
    }
    return status;
  }
  if (g_fragment_profile && reads_this_drain > 0) {
    ++g_frag.drain_data_calls;
    if (reads_this_drain > g_frag.max_reads_per_drain) {
      g_frag.max_reads_per_drain = reads_this_drain;
    }
  }
  return last;
}

InputStatus initial_spin_read_input(int fd, Connection& conn) {
  return spin_read_input(fd, conn, g_fd_initial_read_spins);
}

bool parse_available_requests(Connection& conn) {
  const uint64_t tail_t0 = g_tail_profile ? now_ns() : 0;
  bool parsed = false;
  auto finish = [&]() {
    if (g_tail_profile) {
      const uint64_t elapsed = now_ns() - tail_t0;
      ++g_tail.parse_loops;
      g_tail.parse_loop_ns += elapsed;
      record_tail_ns(elapsed, g_tail.parse_loop_bins, g_tail.parse_loop_max_ns);
    }
    return parsed;
  };
  if (UNLIKELY(g_stop_parse_on_empty && conn.in_start == conn.in_end)) {
    return finish();
  }
  while (true) {
    size_t consumed = 0;
    if (!parse_request(conn, consumed)) {
      if (g_fragment_profile) {
        ++g_frag.parse_waits;
        record_partial_bytes(conn.in_end - conn.in_start);
      }
      break;
    }
    parsed = true;
    size_t available = conn.in_end - conn.in_start;
    if (consumed > 0 && consumed <= available) {
      conn.in_start += consumed;
      if (conn.in_start == conn.in_end) {
        conn.in_start = 0;
        conn.in_end = 0;
      }
    } else {
      conn.in_start = 0;
      conn.in_end = 0;
    }
    if (conn.close_after_write) break;
    if (conn.out_used >= conn.buffers->outbuf.size()) break;
    if (UNLIKELY(g_stop_parse_on_empty && conn.in_start == conn.in_end)) break;
  }
  return finish();
}

void mod_epoll(int epfd, int fd, Connection& conn, uint32_t events, uint64_t event_id) {
  if (g_epoll_interest_cache && conn.epoll_events == events) {
    if (g_event_profile) ++g_event_prof.epoll_mod_skipped;
    return;
  }
  if (g_event_profile) ++g_event_prof.epoll_mod_calls;
  epoll_event ev{};
  ev.events = events;
  ev.data.u64 = event_id;
  if (epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == 0) {
    conn.epoll_events = events;
  } else if (g_event_profile) {
    ++g_event_prof.epoll_mod_errors;
  }
}

enum class FlushStatus {
  Complete,
  Pending,
  Closed,
};

FlushStatus flush_output(int epfd, int slot, Connection& conn, std::vector<Connection>& conns,
                         std::vector<int>& free_slots) {
  const uint64_t tail_t0 = g_tail_profile ? now_ns() : 0;
  auto finish = [&](FlushStatus status) {
    if (g_tail_profile) {
      const uint64_t elapsed = now_ns() - tail_t0;
      ++g_tail.flush_calls;
      g_tail.flush_ns += elapsed;
      record_tail_ns(elapsed, g_tail.flush_bins, g_tail.flush_max_ns);
    }
    return status;
  };
  const int fd = conn.fd;
  const size_t remaining = conn.out_used - conn.out_sent;
  if (UNLIKELY(remaining == 0)) return finish(FlushStatus::Complete);
  const char* out = conn.out_static ? conn.out_static : conn.buffers->outbuf.data();
  ssize_t w = g_rw_syscalls ? write(fd, out + conn.out_sent, remaining)
                             : send(fd, out + conn.out_sent, remaining, MSG_NOSIGNAL);
  if (UNLIKELY(w < 0)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return finish(FlushStatus::Pending);
    release_connection(epfd, slot, conns, free_slots);
    return finish(FlushStatus::Closed);
  }
  if (UNLIKELY(w == 0)) return finish(FlushStatus::Pending);
  conn.out_sent += static_cast<size_t>(w);
  if (UNLIKELY(static_cast<size_t>(w) < remaining)) return finish(FlushStatus::Pending);

  clear_output(conn);
  if (UNLIKELY(conn.close_after_write)) {
    release_connection(epfd, slot, conns, free_slots);
    return finish(FlushStatus::Closed);
  }
  return finish(FlushStatus::Complete);
}

FlushStatus try_post_flush_read(int epfd, int slot, Connection& conn,
                                std::vector<Connection>& conns,
                                std::vector<int>& free_slots,
                                uint64_t event_id) {
  if (g_post_flush_read_spins <= 0 || !conn.active || conn.close_after_write ||
      conn.out_sent < conn.out_used) {
    return FlushStatus::Complete;
  }

  if (g_event_profile) ++g_event_prof.post_flush_attempts;
  const int fd = conn.fd;
  InputStatus input = spin_read_input(fd, conn, g_post_flush_read_spins);
  if (g_event_profile) {
    switch (input) {
      case InputStatus::Data:
        ++g_event_prof.post_flush_data;
        break;
      case InputStatus::Empty:
        ++g_event_prof.post_flush_empty;
        break;
      case InputStatus::Closed:
        ++g_event_prof.post_flush_closed;
        break;
      case InputStatus::BadRequest:
        ++g_event_prof.post_flush_bad;
        break;
    }
  }

  if (input == InputStatus::Closed) {
    release_connection(epfd, slot, conns, free_slots);
    return FlushStatus::Closed;
  }
  if (input != InputStatus::BadRequest) {
    parse_available_requests(conn);
  }
  if (conn.out_sent >= conn.out_used) {
    return FlushStatus::Complete;
  }

  FlushStatus flushed = flush_output(epfd, slot, conn, conns, free_slots);
  if (flushed == FlushStatus::Pending) {
    mod_epoll(epfd, fd, conn, client_readwrite_events(), event_id);
  } else if (flushed == FlushStatus::Complete && g_event_profile) {
    ++g_event_prof.post_flush_responses;
  }
  return flushed;
}

int create_unix_listener(const char* socket_path) {
  int socket_type = g_control_seqpacket ? SOCK_SEQPACKET : SOCK_STREAM;
  int fd = socket(AF_UNIX, socket_type, 0);
  if (UNLIKELY(fd < 0)) return -1;
  set_socket_buffers(fd, g_control_rcvbuf, g_control_sndbuf);
  if (UNLIKELY(set_nonblocking(fd) < 0)) {
    close_fd(fd);
    return -1;
  }
  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  if (UNLIKELY(std::strlen(socket_path) >= sizeof(sa.sun_path))) {
    close_fd(fd);
    return -1;
  }
  std::strncpy(sa.sun_path, socket_path, sizeof(sa.sun_path) - 1);
  unlink(socket_path);
  if (UNLIKELY(bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0)) {
    close_fd(fd);
    return -1;
  }
  chmod(socket_path, 0777);
  if (UNLIKELY(listen(fd, kBacklog) < 0)) {
    close_fd(fd);
    return -1;
  }
  return fd;
}

enum class RecvFdStatus {
  Got,
  Empty,
  Closed,
  Error,
};

RecvFdStatus recv_passed_fds(int sock, int* out_fds, int max_fds, int& out_nfds) {
  out_nfds = 0;
  char byte = 0;
  iovec iov{};
  iov.iov_base = &byte;
  iov.iov_len = 1;

  char control[CMSG_SPACE(sizeof(int) * kMaxControlFdBatch)]{};
  msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = sizeof(control);

  int recv_flags = 0;
  if (g_control_recv_dontwait) recv_flags |= MSG_DONTWAIT;
  if (g_control_recv_cmsg_cloexec) recv_flags |= MSG_CMSG_CLOEXEC;

  ssize_t n;
  for (;;) {
    n = recvmsg(sock, &msg, recv_flags);
    if (n >= 0) break;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return RecvFdStatus::Empty;
    if (errno != EINTR) break;
  }
  if (UNLIKELY(n == 0)) return RecvFdStatus::Closed;
  if (UNLIKELY(n < 0)) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return RecvFdStatus::Empty;
    std::fprintf(stderr, "native control recvmsg failed errno=%d error=%s\n",
                 errno, std::strerror(errno));
    return RecvFdStatus::Error;
  }

  for (cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
    if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS &&
        cmsg->cmsg_len >= CMSG_LEN(sizeof(int))) {
      size_t bytes = cmsg->cmsg_len - CMSG_LEN(0);
      int available = static_cast<int>(bytes / sizeof(int));
      int to_copy = available;
      if (to_copy > max_fds - out_nfds) to_copy = max_fds - out_nfds;
      if (to_copy > 0) {
        std::memcpy(out_fds + out_nfds, CMSG_DATA(cmsg),
                    static_cast<size_t>(to_copy) * sizeof(int));
        out_nfds += to_copy;
        if (out_nfds >= max_fds) break;
      }
    }
  }
  if (out_nfds <= 0) {
    std::fprintf(stderr, "native control recvmsg missing SCM_RIGHTS\n");
    return RecvFdStatus::Error;
  }
  return RecvFdStatus::Got;
}

RecvFdStatus recv_passed_fd(int sock, int& out_fd) {
  int nfds = 0;
  int fds[1]{-1};
  RecvFdStatus status = recv_passed_fds(sock, fds, 1, nfds);
  out_fd = nfds > 0 ? fds[0] : -1;
  return status;
}

int create_tcp_listener(const char* addr_env) {
  std::string addr = addr_env ? addr_env : "0.0.0.0:8081";
  auto pos = addr.rfind(':');
  if (pos == std::string::npos) return -1;
  std::string host = addr.substr(0, pos);
  int port = std::atoi(addr.substr(pos + 1).c_str());
  if (host.empty()) host = "0.0.0.0";

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (g_tcp_reuseport) {
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
  }
  set_socket_buffers(fd, g_tcp_rcvbuf, g_tcp_sndbuf);
  set_tcp_socket_tuning(fd);
  if (g_tcp_listener_nodelay) {
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }
  if (g_tcp_defer_accept_seconds > 0) {
    setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &g_tcp_defer_accept_seconds,
               sizeof(g_tcp_defer_accept_seconds));
  }
  if (set_nonblocking(fd) < 0) {
    close_fd(fd);
    return -1;
  }

  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1) {
    close_fd(fd);
    return -1;
  }
  if (bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
    close_fd(fd);
    return -1;
  }
  if (listen(fd, kBacklog) < 0) {
    close_fd(fd);
    return -1;
  }
  return fd;
}

int create_listener(const char* addr_env) {
  const char* socket_env = std::getenv("SERVICE_SOCKET");
  if (socket_env && socket_env[0] != '\0') {
    return create_unix_listener(socket_env);
  }

  return create_tcp_listener(addr_env);
}

void run_direct_poll_service(int listen_fd, bool is_tcp_listener) {
  std::vector<Connection> conns(static_cast<size_t>(kMaxConnections));
  std::vector<ConnectionBuffers> conn_buffers(static_cast<size_t>(kMaxConnections));
  for (size_t i = 0; i < conns.size(); ++i) {
    conns[i].buffers = &conn_buffers[i];
  }

  std::vector<int> free_slots;
  free_slots.reserve(static_cast<size_t>(kMaxConnections));
  for (int slot = kMaxConnections - 1; slot >= 0; --slot) {
    free_slots.push_back(slot);
  }

  std::vector<pollfd> pfds;
  std::vector<int> pfd_slots;
  pfds.reserve(static_cast<size_t>(kMaxConnections + 1));
  pfd_slots.reserve(static_cast<size_t>(kMaxConnections + 1));
  pfds.push_back(pollfd{listen_fd, POLLIN, 0});
  pfd_slots.push_back(-1);

  auto remove_poll_index = [&](size_t idx) {
    if (idx >= pfds.size()) return;
    int slot = pfd_slots[idx];
    if (slot >= 0 && static_cast<size_t>(slot) < conns.size() && conns[slot].active) {
      release_connection(-1, slot, conns, free_slots);
    }
    const size_t last = pfds.size() - 1;
    if (idx != last) {
      pfds[idx] = pfds[last];
      pfd_slots[idx] = pfd_slots[last];
    }
    pfds.pop_back();
    pfd_slots.pop_back();
  };

  auto add_poll_client = [&](int cfd) {
    if (is_tcp_listener && g_fd_tcp_quickack) {
      int one = 1;
      setsockopt(cfd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
    }
    if (is_tcp_listener && g_tcp_accept_nodelay) {
      int one = 1;
      setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    if (is_tcp_listener && (g_tcp_client_busy_poll || g_tcp_notsent_lowat > 0)) {
      set_tcp_socket_tuning(cfd);
    }
    if (!g_direct_skip_accept4_nonblocking && set_nonblocking(cfd) < 0) {
      close_fd(cfd);
      return;
    }
    if (free_slots.empty() || pfds.size() >= static_cast<size_t>(kMaxConnections + 1)) {
      close_fd(cfd);
      return;
    }
    int slot = free_slots.back();
    free_slots.pop_back();
    reset_connection(conns[static_cast<size_t>(slot)], cfd);
    Connection& conn = conns[static_cast<size_t>(slot)];

    short events = POLLIN;
    if (g_fd_initial_read_spins > 0) {
      InputStatus input = initial_spin_read_input(cfd, conn);
      if (input == InputStatus::Closed) {
        release_connection(-1, slot, conns, free_slots);
        return;
      }
      if (input != InputStatus::BadRequest) {
        parse_available_requests(conn);
      }
      if (conn.out_sent < conn.out_used) {
        FlushStatus flushed = flush_output(-1, slot, conn, conns, free_slots);
        if (flushed == FlushStatus::Closed) return;
        if (flushed == FlushStatus::Pending) {
          events = static_cast<short>(events | POLLOUT);
        }
      }
      if (!conn.active) return;
    }

    pfds.push_back(pollfd{cfd, events, 0});
    pfd_slots.push_back(slot);
  };

  while (!g_stop) {
    poll_tcp_self_warm_child();
    int n = poll(pfds.data(), pfds.size(), -1);
    if (n < 0) {
      if (errno == EINTR) {
        poll_tcp_self_warm_child();
        if (g_stop) break;
        continue;
      }
      std::perror("poll");
      break;
    }
    poll_tcp_self_warm_child();
    if (n == 0) continue;

    for (size_t i = 0; i < pfds.size() && n > 0;) {
      const short re = pfds[i].revents;
      if (re == 0) {
        ++i;
        continue;
      }
      pfds[i].revents = 0;
      --n;

      if (i == 0) {
        if (re & (POLLERR | POLLHUP | POLLNVAL)) {
          g_stop = 1;
          break;
        }
        for (;;) {
          int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break;
          }
          add_poll_client(cfd);
        }
        ++i;
        continue;
      }

      int slot = pfd_slots[i];
      if (slot < 0 || static_cast<size_t>(slot) >= conns.size() || !conns[slot].active) {
        remove_poll_index(i);
        continue;
      }
      Connection& conn = conns[static_cast<size_t>(slot)];
      const int fd = conn.fd;

      if (re & (POLLERR | POLLHUP | POLLNVAL
#ifdef POLLRDHUP
                | POLLRDHUP
#endif
                )) {
        remove_poll_index(i);
        continue;
      }

      if (re & POLLIN) {
        InputStatus input = drain_input(fd, conn);
        if (input == InputStatus::Closed) {
          remove_poll_index(i);
          continue;
        }
        if (input != InputStatus::BadRequest) {
          parse_available_requests(conn);
        }
        if (conn.out_sent < conn.out_used) {
          FlushStatus flushed = flush_output(-1, slot, conn, conns, free_slots);
          if (flushed == FlushStatus::Closed) {
            remove_poll_index(i);
            continue;
          }
        }
      }

      if (conn.active && (re & POLLOUT) && conn.out_sent < conn.out_used) {
        FlushStatus flushed = flush_output(-1, slot, conn, conns, free_slots);
        if (flushed == FlushStatus::Closed) {
          remove_poll_index(i);
          continue;
        }
      }

      if (!conn.active) {
        remove_poll_index(i);
        continue;
      }
      pfds[i].events = conn.out_sent < conn.out_used
                           ? static_cast<short>(POLLIN | POLLOUT)
                           : static_cast<short>(POLLIN);
      ++i;
    }
  }

  for (size_t i = 1; i < pfds.size(); ++i) {
    int slot = pfd_slots[i];
    if (slot >= 0 && static_cast<size_t>(slot) < conns.size() && conns[slot].active) {
      release_connection(-1, slot, conns, free_slots);
    }
  }
  close_fd(listen_fd);
}

constexpr int kUringEntries = 8192;

enum UringEvent : uint8_t {
  kUringAccept = 1,
  kUringRead = 2,
  kUringWrite = 3,
};

uint64_t make_uring_event(UringEvent type, int slot, uint32_t generation) {
  return (static_cast<uint64_t>(generation) << 32) |
         (static_cast<uint64_t>(static_cast<uint32_t>(slot + 1)) << 8) |
         static_cast<uint64_t>(type);
}

UringEvent uring_event_type(uint64_t user_data) {
  return static_cast<UringEvent>(user_data & 0xffu);
}

int uring_event_slot(uint64_t user_data) {
  return static_cast<int>((user_data >> 8) & 0x00ffffffu) - 1;
}

uint32_t uring_event_generation(uint64_t user_data) {
  return static_cast<uint32_t>(user_data >> 32);
}

struct RawIoUring {
  int fd = -1;
  io_uring_params params{};
  void* sq_ptr = MAP_FAILED;
  void* cq_ptr = MAP_FAILED;
  io_uring_sqe* sqes = nullptr;
  size_t sq_ring_size = 0;
  size_t cq_ring_size = 0;
  size_t sqes_size = 0;
  uint32_t* sq_head = nullptr;
  uint32_t* sq_tail = nullptr;
  uint32_t* sq_ring_mask = nullptr;
  uint32_t* sq_ring_entries = nullptr;
  uint32_t* sq_array = nullptr;
  uint32_t* cq_head = nullptr;
  uint32_t* cq_tail = nullptr;
  uint32_t* cq_ring_mask = nullptr;
  io_uring_cqe* cqes = nullptr;
  uint32_t pending_submit = 0;

  bool setup(uint32_t entries) {
    std::memset(&params, 0, sizeof(params));
    fd = static_cast<int>(syscall(__NR_io_uring_setup, entries, &params));
    if (fd < 0) return false;

    sq_ring_size = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
    cq_ring_size = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
    if (params.features & IORING_FEAT_SINGLE_MMAP) {
      if (cq_ring_size > sq_ring_size) sq_ring_size = cq_ring_size;
      cq_ring_size = sq_ring_size;
    }

    sq_ptr = mmap(nullptr, sq_ring_size, PROT_READ | PROT_WRITE,
                  MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (sq_ptr == MAP_FAILED) return false;
    if (params.features & IORING_FEAT_SINGLE_MMAP) {
      cq_ptr = sq_ptr;
    } else {
      cq_ptr = mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
      if (cq_ptr == MAP_FAILED) return false;
    }

    sqes_size = params.sq_entries * sizeof(io_uring_sqe);
    sqes = static_cast<io_uring_sqe*>(mmap(nullptr, sqes_size, PROT_READ | PROT_WRITE,
                                           MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES));
    if (sqes == MAP_FAILED) {
      sqes = nullptr;
      return false;
    }

    auto* sq = static_cast<char*>(sq_ptr);
    auto* cq = static_cast<char*>(cq_ptr);
    sq_head = reinterpret_cast<uint32_t*>(sq + params.sq_off.head);
    sq_tail = reinterpret_cast<uint32_t*>(sq + params.sq_off.tail);
    sq_ring_mask = reinterpret_cast<uint32_t*>(sq + params.sq_off.ring_mask);
    sq_ring_entries = reinterpret_cast<uint32_t*>(sq + params.sq_off.ring_entries);
    sq_array = reinterpret_cast<uint32_t*>(sq + params.sq_off.array);
    cq_head = reinterpret_cast<uint32_t*>(cq + params.cq_off.head);
    cq_tail = reinterpret_cast<uint32_t*>(cq + params.cq_off.tail);
    cq_ring_mask = reinterpret_cast<uint32_t*>(cq + params.cq_off.ring_mask);
    cqes = reinterpret_cast<io_uring_cqe*>(cq + params.cq_off.cqes);
    return true;
  }

  void close_ring() {
    if (sqes) {
      munmap(sqes, sqes_size);
      sqes = nullptr;
    }
    if (cq_ptr != MAP_FAILED && cq_ptr != sq_ptr) {
      munmap(cq_ptr, cq_ring_size);
      cq_ptr = MAP_FAILED;
    }
    if (sq_ptr != MAP_FAILED) {
      munmap(sq_ptr, sq_ring_size);
      sq_ptr = MAP_FAILED;
    }
    if (fd >= 0) {
      close_fd(fd);
      fd = -1;
    }
  }

  io_uring_sqe* get_sqe() {
    const uint32_t head = *sq_head;
    const uint32_t tail = *sq_tail;
    if (tail - head >= *sq_ring_entries) return nullptr;
    const uint32_t index = tail & *sq_ring_mask;
    io_uring_sqe* sqe = &sqes[index];
    std::memset(sqe, 0, sizeof(*sqe));
    sq_array[index] = index;
    *sq_tail = tail + 1;
    ++pending_submit;
    return sqe;
  }

  bool submit_and_wait(uint32_t min_complete) {
    for (;;) {
      const uint32_t to_submit = pending_submit;
      const unsigned flags = min_complete > 0 ? IORING_ENTER_GETEVENTS : 0;
      long ret = syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, nullptr, 0);
      if (ret >= 0) {
        pending_submit = 0;
        return true;
      }
      if (errno == EINTR || errno == EAGAIN) continue;
      return false;
    }
  }
};

bool queue_uring_accept(RawIoUring& ring, int listen_fd) {
  io_uring_sqe* sqe = ring.get_sqe();
  if (!sqe) return false;
  sqe->opcode = IORING_OP_ACCEPT;
  sqe->fd = listen_fd;
  sqe->addr = 0;
  sqe->addr2 = 0;
  sqe->accept_flags = SOCK_NONBLOCK | SOCK_CLOEXEC;
  sqe->user_data = static_cast<uint64_t>(kUringAccept);
  return true;
}

bool queue_uring_send(RawIoUring& ring, int slot, Connection& conn) {
  const size_t remaining = conn.out_used - conn.out_sent;
  if (remaining == 0) return true;
  io_uring_sqe* sqe = ring.get_sqe();
  if (!sqe) return false;
  const char* out = conn.out_static ? conn.out_static : conn.buffers->outbuf.data();
  sqe->opcode = IORING_OP_SEND;
  sqe->fd = conn.fd;
  sqe->addr = reinterpret_cast<uint64_t>(out + conn.out_sent);
  sqe->len = static_cast<uint32_t>(remaining);
  sqe->msg_flags = MSG_NOSIGNAL;
  sqe->user_data = make_uring_event(kUringWrite, slot, conn.generation);
  return true;
}

bool queue_uring_recv(RawIoUring& ring, int slot, Connection& conn) {
  compact_input(conn);
  if (conn.in_end >= conn.buffers->inbuf.size()) {
    queue_bad_request(conn);
    return queue_uring_send(ring, slot, conn);
  }
  io_uring_sqe* sqe = ring.get_sqe();
  if (!sqe) return false;
  sqe->opcode = IORING_OP_RECV;
  sqe->fd = conn.fd;
  sqe->addr = reinterpret_cast<uint64_t>(conn.buffers->inbuf.data() + conn.in_end);
  sqe->len = static_cast<uint32_t>(conn.buffers->inbuf.size() - conn.in_end);
  sqe->msg_flags = 0;
  sqe->user_data = make_uring_event(kUringRead, slot, conn.generation);
  return true;
}

void release_uring_connection(int slot, std::vector<Connection>& conns,
                              std::vector<int>& free_slots) {
  release_connection(-1, slot, conns, free_slots);
}

bool uring_after_parse(RawIoUring& ring, int slot, std::vector<Connection>& conns,
                       std::vector<int>& free_slots) {
  if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) return true;
  Connection& conn = conns[static_cast<size_t>(slot)];
  if (!conn.active) return true;
  if (conn.out_sent < conn.out_used) {
    if (!queue_uring_send(ring, slot, conn)) {
      release_uring_connection(slot, conns, free_slots);
      return false;
    }
    return true;
  }
  if (!queue_uring_recv(ring, slot, conn)) {
    release_uring_connection(slot, conns, free_slots);
    return false;
  }
  return true;
}

void run_io_uring_direct_service(int listen_fd, bool is_tcp_listener) {
  RawIoUring ring;
  if (!ring.setup(kUringEntries)) {
    std::perror("io_uring_setup");
    ring.close_ring();
    close_fd(listen_fd);
    return;
  }

  std::vector<Connection> conns(static_cast<size_t>(kMaxConnections));
  std::vector<ConnectionBuffers> conn_buffers(static_cast<size_t>(kMaxConnections));
  for (size_t i = 0; i < conns.size(); ++i) {
    conns[i].buffers = &conn_buffers[i];
  }
  std::vector<int> free_slots;
  free_slots.reserve(static_cast<size_t>(kMaxConnections));
  for (int slot = kMaxConnections - 1; slot >= 0; --slot) {
    free_slots.push_back(slot);
  }

  if (!queue_uring_accept(ring, listen_fd)) {
    ring.close_ring();
    close_fd(listen_fd);
    return;
  }

  std::fprintf(stderr, "native io_uring direct service enabled entries=%d features=0x%x\n",
               kUringEntries, ring.params.features);

  while (!g_stop) {
    poll_tcp_self_warm_child();
    if (!ring.submit_and_wait(1)) {
      std::perror("io_uring_enter");
      break;
    }
    poll_tcp_self_warm_child();

    uint32_t head = *ring.cq_head;
    const uint32_t tail = *ring.cq_tail;
    while (head != tail) {
      io_uring_cqe cqe = ring.cqes[head & *ring.cq_ring_mask];
      ++head;

      const UringEvent type = uring_event_type(cqe.user_data);
      if (type == kUringAccept) {
        if (!queue_uring_accept(ring, listen_fd)) {
          g_stop = 1;
          continue;
        }
        if (cqe.res < 0) {
          continue;
        }
        int cfd = cqe.res;
        if (is_tcp_listener && g_fd_tcp_quickack) {
          int one = 1;
          setsockopt(cfd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
        }
        if (is_tcp_listener && g_tcp_accept_nodelay) {
          int one = 1;
          setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        }
        if (is_tcp_listener && (g_tcp_client_busy_poll || g_tcp_notsent_lowat > 0)) {
          set_tcp_socket_tuning(cfd);
        }
        if (free_slots.empty()) {
          close_fd(cfd);
          continue;
        }
        int slot = free_slots.back();
        free_slots.pop_back();
        reset_connection(conns[static_cast<size_t>(slot)], cfd);
        if (!queue_uring_recv(ring, slot, conns[static_cast<size_t>(slot)])) {
          release_uring_connection(slot, conns, free_slots);
        }
        continue;
      }

      const int slot = uring_event_slot(cqe.user_data);
      if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) continue;
      Connection& conn = conns[static_cast<size_t>(slot)];
      if (!conn.active || conn.generation != uring_event_generation(cqe.user_data)) continue;

      if (type == kUringRead) {
        if (cqe.res <= 0) {
          release_uring_connection(slot, conns, free_slots);
          continue;
        }
        conn.in_end += static_cast<size_t>(cqe.res);
        parse_available_requests(conn);
        uring_after_parse(ring, slot, conns, free_slots);
        continue;
      }

      if (type == kUringWrite) {
        if (cqe.res <= 0) {
          release_uring_connection(slot, conns, free_slots);
          continue;
        }
        conn.out_sent += static_cast<size_t>(cqe.res);
        if (conn.out_sent < conn.out_used) {
          if (!queue_uring_send(ring, slot, conn)) {
            release_uring_connection(slot, conns, free_slots);
          }
          continue;
        }
        clear_output(conn);
        if (conn.close_after_write) {
          release_uring_connection(slot, conns, free_slots);
          continue;
        }
        parse_available_requests(conn);
        uring_after_parse(ring, slot, conns, free_slots);
      }
    }
    *ring.cq_head = head;
  }

  for (Connection& conn : conns) {
    if (conn.active) {
      close_fd(conn.fd);
      clear_connection(conn);
    }
  }
  ring.close_ring();
  close_fd(listen_fd);
}

}  // namespace

int main() {
  signal(SIGPIPE, SIG_IGN);
  signal(SIGTERM, handle_stop);
  signal(SIGINT, handle_stop);
  g_fake_classifier = env_equals(std::getenv("NATIVE_CLASSIFIER_MODE"), "fake");
  g_native_tier_path = env_bool("NATIVE_TIER_PATH", false);
  g_cpp_ivf = env_equals(std::getenv("NATIVE_IVF_MODE"), "cpp") && !g_native_tier_path;
  g_ivf_trace = env_equals(std::getenv("NATIVE_IVF_TRACE"), "1");
  g_cpp_vector_parser = env_equals(std::getenv("VECTOR_MODE"), "native") ||
                        env_equals(std::getenv("NATIVE_VECTOR_MODE"), "cpp") ||
                        g_cpp_ivf;
  const char* profile_env = std::getenv("NATIVE_PROFILE");
  g_profile = env_equals(profile_env, "1") || env_equals(profile_env, "tail") ||
              env_equals(std::getenv("NATIVE_IVF_MARGIN_PROFILE"), "1") ||
              env_equals(std::getenv("NATIVE_IVF_CENTROID_FLIP_PROFILE"), "1");
  g_tail_profile = env_bool("NATIVE_TAIL_PROFILE", false) || env_equals(profile_env, "tail");
  g_event_profile = env_bool("NATIVE_EVENT_PROFILE", false);
  const char* fd_control_socket = std::getenv("FD_CONTROL_SOCKET");
  g_fd_receiver = fd_control_socket && fd_control_socket[0] != '\0';
  g_skip_fd_nonblocking = env_bool("NATIVE_SKIP_FD_NONBLOCKING", true);
  g_aggressive_compact = env_bool("NATIVE_AGGRESSIVE_COMPACT", true);
  g_fragment_profile = env_bool("NATIVE_FRAGMENT_PROFILE", false);
  g_rw_syscalls = env_bool("NATIVE_RW_SYSCALLS", false);
  g_direct_static_response = env_bool("NATIVE_DIRECT_STATIC_RESPONSE", false);
  g_direct_complete_post = env_bool("NATIVE_DIRECT_COMPLETE_POST", false);
  g_fd_tcp_quickack = env_bool("NATIVE_FD_TCP_QUICKACK", false);
  g_native_fast_path = env_bool("NATIVE_FAST_PATH", false);
  if (g_native_fast_path) {
    if (env_bool("NATIVE_FAST_PATH_LEGIT", true)) {
      g_native_fast_path_mode |= kNativeFastPathLegit;
    }
    if (env_bool("NATIVE_FAST_PATH_FRAUD", false)) {
      g_native_fast_path_mode |= kNativeFastPathFraud;
    }
    if (env_bool("NATIVE_FAST_PATH_EXTREME_FRAUD", false)) {
      g_native_fast_path_mode |= kNativeFastPathExtremeFraud;
    }
    if (env_bool("NATIVE_FAST_PATH_FRAUD_OFFLINE", false)) {
      g_native_fast_path_mode |= kNativeFastPathFraudOffline;
    }
    if (env_bool("NATIVE_FAST_PATH_TREE", false)) {
      g_native_fast_path_mode |= kNativeFastPathTree;
      if (env_bool("NATIVE_FAST_PATH_TREE_FRAUD_UNKNOWN", true)) {
        g_native_fast_path_mode |= kNativeFastPathTreeFraudUnknown;
      }
    }
    if (g_native_fast_path_mode == 0) g_native_fast_path = false;
  }
  g_native_fast_fraud_ivf_route =
      g_native_fast_path && env_bool("NATIVE_FAST_PATH_FRAUD_IVF", false);
  g_native_fast_fraud_ivf_quick_probe =
      env_int("NATIVE_FAST_PATH_FRAUD_IVF_QUICK", 4, 1, 512);
  g_tcp_rcvbuf = env_int("NATIVE_TCP_RCVBUF", 0, 0, 4 * 1024 * 1024);
  g_tcp_sndbuf = env_int("NATIVE_TCP_SNDBUF", 0, 0, 4 * 1024 * 1024);
  g_control_rcvbuf = env_int("NATIVE_CONTROL_RCVBUF", 0, 0, 1024 * 1024);
  g_control_sndbuf = env_int("NATIVE_CONTROL_SNDBUF", 0, 0, 1024 * 1024);
  g_control_seqpacket = env_bool("NATIVE_CONTROL_SEQPACKET", false);
  g_control_recv_dontwait =
      env_bool("NATIVE_CONTROL_RECV_DONTWAIT", env_bool("FD_CONTROL_RECV_DONTWAIT", false));
  g_control_recv_cmsg_cloexec =
      env_bool("NATIVE_CONTROL_RECV_CMSG_CLOEXEC", env_bool("FD_CONTROL_CMSG_CLOEXEC", false));
  g_control_loop_warn = env_bool("NATIVE_CONTROL_LOOP_WARN", false);
  g_epoll_interest_cache = env_bool("NATIVE_EPOLL_INTEREST_CACHE", false);
  g_fd_control_connections = env_int("NATIVE_FD_CONTROL_CONNECTIONS", 1, 1, kMaxControlConnections);
  g_epoll_busy_poll_us = env_int("NATIVE_EPOLL_BUSY_POLL_US", 0, 0, 1000);
  g_epoll_busy_poll_budget = env_int("NATIVE_EPOLL_BUSY_POLL_BUDGET", 8, 1, 256);
  g_epoll_prefer_busy_poll = env_int("NATIVE_EPOLL_PREFER_BUSY_POLL", 1, 0, 1);
  g_epoll_spin_us = env_int("NATIVE_EPOLL_SPIN_US", 0, 0, 1000);
  g_epoll_idle_us = env_int("NATIVE_EPOLL_IDLE_US", 0, 0, 10000);
  g_epoll_idle_max_us = env_int("NATIVE_EPOLL_IDLE_MAX_US", 0, 0, 10000);
  g_epoll_idle_active_window_us =
      env_int("NATIVE_EPOLL_IDLE_ACTIVE_WINDOW_US", 0, 0, 1000000);
  g_epoll_timeout_backoff_after =
      env_int("NATIVE_EPOLL_TIMEOUT_BACKOFF_AFTER", 0, 0, 1000000);
  g_epoll_timeout_backoff_short_waits =
      env_int("NATIVE_EPOLL_TIMEOUT_BACKOFF_SHORT_WAITS", 1, 0, 1000000);
  g_mlockall = env_bool("NATIVE_MLOCKALL", false);
  g_startup_warmup_requests = env_int("NATIVE_STARTUP_WARMUP_REQUESTS", 0, 0, 100000);
  g_fd_initial_read_spins = env_int("NATIVE_FD_INITIAL_READ_SPINS", 0, 0, 128);
  g_post_flush_read_spins = env_int("NATIVE_POST_FLUSH_READ_SPINS", 0, 0, 128);
  g_fd_control_single_recv = env_bool("NATIVE_FD_CONTROL_SINGLE_RECV", false);
  g_fd_client_level_trigger = env_bool("NATIVE_FD_CLIENT_LEVEL_TRIGGER", false);
  g_fd_direct_slots = env_bool("NATIVE_FD_DIRECT_SLOTS", false);
  g_timer_slack_ns = env_int("NATIVE_TIMER_SLACK_NS", 0, 0, 1000000);
  g_sched_fifo_priority = env_int("NATIVE_SCHED_FIFO_PRIORITY", 0, 0, 99);
  g_set_nice_value = env_int_present("NATIVE_NICE", g_nice_value, -20, 19);
  g_tcp_workers = env_int("NATIVE_TCP_WORKERS", 1, 1, 8);
  g_tcp_defer_accept_seconds = env_int("NATIVE_TCP_DEFER_ACCEPT_SECONDS", 0, 0, 60);
  g_tcp_socket_busy_poll_us = env_int("NATIVE_TCP_SOCKET_BUSY_POLL_US", 0, 0, 1000);
  g_tcp_socket_busy_poll_budget = env_int("NATIVE_TCP_SOCKET_BUSY_POLL_BUDGET", 0, 0, 256);
  g_tcp_socket_prefer_busy_poll = env_int("NATIVE_TCP_SOCKET_PREFER_BUSY_POLL", 0, 0, 1);
  g_tcp_client_busy_poll = env_int("NATIVE_TCP_CLIENT_BUSY_POLL", 0, 0, 1);
  g_tcp_notsent_lowat = env_int("NATIVE_TCP_NOTSENT_LOWAT", 0, 0, 65535);
  g_tcp_reuseport = env_bool("NATIVE_TCP_REUSEPORT", false);
  g_tcp_listener_nodelay = env_bool("NATIVE_TCP_LISTENER_NODELAY", true);
  g_tcp_accept_nodelay = env_bool("NATIVE_TCP_ACCEPT_NODELAY", true);
  g_direct_skip_accept4_nonblocking = env_bool("NATIVE_DIRECT_SKIP_ACCEPT4_NONBLOCKING", true);
  g_direct_poll_service = env_bool("NATIVE_DIRECT_POLL_SERVICE", false);
  g_single_recv_per_event = env_bool("NATIVE_SINGLE_RECV_PER_EVENT", false);
  g_stop_parse_on_empty = env_bool("NATIVE_STOP_PARSE_ON_EMPTY", false);
  g_io_uring_direct = env_bool("NATIVE_IO_URING_DIRECT", false);
  g_tcp_self_warm_requests = env_int("NATIVE_TCP_SELF_WARM_REQUESTS", 0, 0, 10000);
  g_tcp_self_warm_delay_ms = env_int("NATIVE_TCP_SELF_WARM_DELAY_MS", 20, 0, 10000);
  if (g_timer_slack_ns > 0) {
    prctl(PR_SET_TIMERSLACK, g_timer_slack_ns, 0, 0, 0);
  }
  apply_scheduler_tuning();
  if (g_mlockall) {
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
      std::perror("mlockall");
    } else {
      std::fprintf(stderr, "native mlockall enabled\n");
    }
  }
  if (g_cpp_ivf) {
    const char* index_path = std::getenv("NATIVE_INDEX_PATH");
    if (!index_path || index_path[0] == '\0') {
      index_path = "service/index.bin";
    }
    if (!g_native_ivf.load(index_path)) {
      std::fprintf(stderr, "native C++ IVF load failed: %s\n", g_native_ivf.error().c_str());
      return 1;
    }
    g_native_ivf.set_profile(false);
    const uint64_t warm_start = now_ns();
    g_native_ivf.warmup();
    const uint64_t warm_end = now_ns();
    g_native_ivf.set_profile(g_profile);
    std::fprintf(stderr, "native C++ IVF warmup finished in %.3fms\n",
                 static_cast<double>(warm_end - warm_start) / 1000000.0);
    std::fprintf(stderr, "native C++ IVF enabled\n");
  }
  if (!g_fake_classifier && !g_cpp_ivf && !g_native_tier_path && fraud_init() != 0) {
    std::fprintf(stderr, "native bridge init failed\n");
    return 1;
  }
  if (g_fake_classifier) {
    std::fprintf(stderr, "native fake classifier mode enabled\n");
  }
  if (!g_fake_classifier && g_cpp_vector_parser) {
    std::fprintf(stderr, "native C++ vector parser enabled\n");
  }
  if (g_profile) {
    std::fprintf(stderr, "native tail profile enabled\n");
  }
  run_startup_warmup();

  const char* addr = std::getenv("SERVICE_ADDR");
  const char* socket_path = std::getenv("SERVICE_SOCKET");
  if (!g_fd_receiver && !(socket_path && socket_path[0] != '\0') && g_tcp_workers > 1) {
    g_tcp_reuseport = true;
    for (int worker = 1; worker < g_tcp_workers; ++worker) {
      pid_t pid = fork();
      if (pid == 0) break;
      if (pid < 0) {
        std::perror("fork");
        break;
      }
    }
  }
  int listen_fd = g_fd_receiver ? create_unix_listener(fd_control_socket) : create_listener(addr);
  if (listen_fd < 0) {
    std::perror("listen");
    return 1;
  }

  int epfd = epoll_create1(0);
  if (epfd < 0) {
    std::perror("epoll_create1");
    close_fd(listen_fd);
    return 1;
  }
  if (g_epoll_busy_poll_us > 0) {
    epoll_params params{};
    params.busy_poll_usecs = static_cast<uint32_t>(g_epoll_busy_poll_us);
    params.busy_poll_budget = static_cast<uint16_t>(g_epoll_busy_poll_budget);
    params.prefer_busy_poll = static_cast<uint8_t>(g_epoll_prefer_busy_poll);
    if (ioctl(epfd, EPIOCSPARAMS, &params) != 0) {
      std::fprintf(stderr, "epoll busy-poll ioctl failed: %s\n", std::strerror(errno));
    }
  }

  epoll_event listen_ev{};
  listen_ev.events = EPOLLIN | EPOLLET;
  listen_ev.data.u64 = kListenEventId;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &listen_ev) < 0) {
    std::perror("epoll_ctl add listen");
    close_fd(epfd);
    close_fd(listen_fd);
    return 1;
  }

  std::fprintf(stderr, "native %s service running on %s\n",
               (g_io_uring_direct && !g_fd_receiver) ? "io_uring" :
               ((g_direct_poll_service && !g_fd_receiver) ? "poll" : "epoll"),
               g_fd_receiver ? fd_control_socket :
               (socket_path && socket_path[0] != '\0' ? socket_path : (addr ? addr : "0.0.0.0:8081")));
  std::fprintf(stderr,
               "native opts fd-receiver=%d skip-fd-nonblocking=%d aggressive-compact=%d "
               "fragment-profile=%d rw-syscalls=%d tcp-rcvbuf=%d tcp-sndbuf=%d "
               "control-rcvbuf=%d control-sndbuf=%d control-seqpacket=%d "
               "control-recv-dontwait=%d control-recv-cmsg-cloexec=%d control-loop-warn=%d "
               "epoll-interest-cache=%d native-fast-path=%d "
               "native-fast-mode=%d native-fast-fraud-ivf=%d "
               "native-fast-fraud-ivf-quick=%d native-tier-path=%d fd-initial-read-spins=%d "
               "post-flush-read-spins=%d fd-control-connections=%d "
               "fd-control-single-recv=%d fd-client-level-trigger=%d fd-direct-slots=%d "
               "epoll-spin-us=%d epoll-idle-us=%d epoll-idle-max-us=%d "
               "epoll-idle-active-window-us=%d epoll-timeout-backoff-after=%d "
               "epoll-timeout-backoff-short-waits=%d timer-slack-ns=%d "
               "sched-fifo-priority=%d nice-set=%d nice-value=%d "
               "tcp-workers=%d tcp-reuseport=%d "
               "tcp-defer-accept=%d tcp-socket-busy-poll-us=%d "
               "tcp-socket-busy-poll-budget=%d tcp-socket-prefer-busy-poll=%d "
               "tcp-client-busy-poll=%d tcp-notsent-lowat=%d "
               "tcp-listener-nodelay=%d tcp-accept-nodelay=%d "
               "direct-skip-accept4-nonblocking=%d direct-poll-service=%d "
               "single-recv-per-event=%d stop-parse-on-empty=%d "
               "io-uring-direct=%d tcp-self-warm-requests=%d tcp-self-warm-delay-ms=%d\n",
               g_fd_receiver ? 1 : 0, g_skip_fd_nonblocking ? 1 : 0,
               g_aggressive_compact ? 1 : 0, g_fragment_profile ? 1 : 0,
               g_rw_syscalls ? 1 : 0,
               g_tcp_rcvbuf, g_tcp_sndbuf, g_control_rcvbuf, g_control_sndbuf,
               g_control_seqpacket ? 1 : 0,
               g_control_recv_dontwait ? 1 : 0,
               g_control_recv_cmsg_cloexec ? 1 : 0,
               g_control_loop_warn ? 1 : 0,
               g_epoll_interest_cache ? 1 : 0,
               g_native_fast_path ? 1 : 0, g_native_fast_path_mode,
               g_native_fast_fraud_ivf_route ? 1 : 0,
               g_native_fast_fraud_ivf_quick_probe,
               g_native_tier_path ? 1 : 0, g_fd_initial_read_spins,
               g_post_flush_read_spins, g_fd_control_connections,
               g_fd_control_single_recv ? 1 : 0,
               g_fd_client_level_trigger ? 1 : 0,
               g_fd_direct_slots ? 1 : 0, g_epoll_spin_us, g_epoll_idle_us,
               g_epoll_idle_max_us, g_epoll_idle_active_window_us,
               g_epoll_timeout_backoff_after, g_epoll_timeout_backoff_short_waits,
               g_timer_slack_ns, g_sched_fifo_priority, g_set_nice_value ? 1 : 0,
               g_nice_value, g_tcp_workers, g_tcp_reuseport ? 1 : 0,
               g_tcp_defer_accept_seconds, g_tcp_socket_busy_poll_us,
               g_tcp_socket_busy_poll_budget, g_tcp_socket_prefer_busy_poll,
               g_tcp_client_busy_poll, g_tcp_notsent_lowat,
               g_tcp_listener_nodelay ? 1 : 0, g_tcp_accept_nodelay ? 1 : 0,
               g_direct_skip_accept4_nonblocking ? 1 : 0,
               g_direct_poll_service ? 1 : 0,
               g_single_recv_per_event ? 1 : 0,
               g_stop_parse_on_empty ? 1 : 0,
               g_io_uring_direct ? 1 : 0,
               g_tcp_self_warm_requests, g_tcp_self_warm_delay_ms);

  if (!g_fd_receiver && !(socket_path && socket_path[0] != '\0')) {
    start_tcp_self_warm_child(addr);
  }

  if (g_io_uring_direct && !g_fd_receiver) {
    close_fd(epfd);
    run_io_uring_direct_service(listen_fd, !(socket_path && socket_path[0] != '\0'));
    print_profile();
    print_tail_profile();
    print_fragment_profile();
    return 0;
  }

  if (g_direct_poll_service && !g_fd_receiver) {
    close_fd(epfd);
    std::fprintf(stderr, "native direct poll service enabled\n");
    run_direct_poll_service(listen_fd, !(socket_path && socket_path[0] != '\0'));
    print_profile();
    print_tail_profile();
    print_fragment_profile();
    return 0;
  }

  std::vector<Connection> conns(static_cast<size_t>(kMaxConnections));
  std::vector<ConnectionBuffers> conn_buffers(static_cast<size_t>(kMaxConnections));
  for (size_t i = 0; i < conns.size(); ++i) {
    conns[i].buffers = &conn_buffers[i];
  }
  std::vector<int> free_slots;
  free_slots.reserve(static_cast<size_t>(kMaxConnections));
  for (int slot = kMaxConnections - 1; slot >= 0; --slot) {
    free_slots.push_back(slot);
  }
  std::array<epoll_event, kMaxEvents> events{};
  std::array<int, kMaxControlConnections> control_fds{};
  control_fds.fill(-1);

  auto close_control_slot = [&](int slot) {
    if (slot < 0 || slot >= kMaxControlConnections) return;
    int& control_fd = control_fds[static_cast<size_t>(slot)];
    if (control_fd >= 0) {
      epoll_ctl(epfd, EPOLL_CTL_DEL, control_fd, nullptr);
      close_fd(control_fd);
      control_fd = -1;
    }
  };

  auto close_all_controls = [&]() {
    for (int slot = 0; slot < kMaxControlConnections; ++slot) {
      close_control_slot(slot);
    }
  };

  auto add_control = [&](int cfd) {
    if (set_nonblocking(cfd) < 0) {
      close_fd(cfd);
      return;
    }
    int slot = -1;
    for (int i = 0; i < g_fd_control_connections; ++i) {
      if (control_fds[static_cast<size_t>(i)] < 0) {
        slot = i;
        break;
      }
    }
    if (slot < 0 && g_fd_control_connections == 1) {
      close_control_slot(0);
      slot = 0;
    }
    if (slot < 0) {
      close_fd(cfd);
      return;
    }

    control_fds[static_cast<size_t>(slot)] = cfd;
    epoll_event cev{};
    cev.events = kReadEvents;
    cev.data.u64 = make_control_event_id(slot);
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) < 0) {
      close_control_slot(slot);
    }
  };

  auto add_client = [&](int cfd, bool is_tcp) {
    if (g_event_profile) ++g_event_prof.fd_clients;
    if (is_tcp && g_fd_tcp_quickack) {
      int one = 1;
      setsockopt(cfd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
    }
    if (is_tcp && !g_fd_receiver && g_tcp_accept_nodelay) {
      int one = 1;
      setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    if (is_tcp && !g_fd_receiver && (g_tcp_client_busy_poll || g_tcp_notsent_lowat > 0)) {
      set_tcp_socket_tuning(cfd);
    }
    const bool needs_nonblocking =
        g_fd_receiver ? !g_skip_fd_nonblocking : !g_direct_skip_accept4_nonblocking;
    if (needs_nonblocking && set_nonblocking(cfd) < 0) {
      close_fd(cfd);
      return;
    }
    int slot = -1;
    if (g_fd_direct_slots) {
      if (cfd < 0 || cfd >= kMaxConnections || conns[static_cast<size_t>(cfd)].active) {
        close_fd(cfd);
        return;
      }
      slot = cfd;
    } else {
      if (free_slots.empty()) {
        close_fd(cfd);
        return;
      }
      slot = free_slots.back();
      free_slots.pop_back();
    }
    reset_connection(conns[static_cast<size_t>(slot)], cfd);
    Connection& conn = conns[static_cast<size_t>(slot)];
    uint32_t client_events = client_read_events();
    if (is_tcp && g_fd_initial_read_spins > 0) {
      InputStatus input = initial_spin_read_input(cfd, conn);
      if (g_event_profile) {
        ++g_event_prof.initial_read_clients;
        switch (input) {
          case InputStatus::Data:
            ++g_event_prof.initial_read_data;
            break;
          case InputStatus::Empty:
            ++g_event_prof.initial_read_empty;
            break;
          case InputStatus::Closed:
            ++g_event_prof.initial_read_closed;
            break;
          case InputStatus::BadRequest:
            ++g_event_prof.initial_read_bad;
            break;
        }
      }
      if (input == InputStatus::Closed) {
        release_connection(epfd, slot, conns, free_slots);
        return;
      }
      if (input != InputStatus::BadRequest) {
        parse_available_requests(conn);
      }
      if (conn.out_sent < conn.out_used) {
        FlushStatus flushed = flush_output(epfd, slot, conn, conns, free_slots);
        if (flushed == FlushStatus::Closed) return;
        if (flushed == FlushStatus::Pending) {
          client_events = client_readwrite_events();
        }
      }
      if (!conn.active) return;
    }
    epoll_event cev{};
    cev.events = client_events;
    cev.data.u64 = make_client_event_id(slot, conn.generation);
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) == 0) {
      conn.epoll_events = client_events;
    } else {
      release_connection(epfd, slot, conns, free_slots);
    }
  };

  const bool adaptive_epoll_idle =
      g_epoll_idle_us > 0 && g_epoll_idle_max_us > g_epoll_idle_us &&
      g_epoll_idle_active_window_us > 0 && g_epoll_timeout_backoff_after <= 0;
  const bool timeout_backoff_epoll_idle =
      g_epoll_idle_us > 0 && g_epoll_idle_max_us > g_epoll_idle_us &&
      g_epoll_timeout_backoff_after > 0;
  uint64_t last_epoll_activity_ns = adaptive_epoll_idle ? now_ns() : 0;
  int consecutive_epoll_timeouts = 0;
  int epoll_short_waits_remaining = 0;

  while (!g_stop) {
    poll_tcp_self_warm_child();
    int idle_us = g_epoll_idle_us;
    if (timeout_backoff_epoll_idle) {
      if (epoll_short_waits_remaining <= 0 &&
          consecutive_epoll_timeouts >= g_epoll_timeout_backoff_after) {
        idle_us = g_epoll_idle_max_us;
      }
    } else if (adaptive_epoll_idle) {
      const uint64_t idle_for_ns = now_ns() - last_epoll_activity_ns;
      if (idle_for_ns >
          static_cast<uint64_t>(g_epoll_idle_active_window_us) * 1000ull) {
        idle_us = g_epoll_idle_max_us;
      }
    }
    if (g_event_profile && g_epoll_idle_max_us > g_epoll_idle_us) {
      if (idle_us >= g_epoll_idle_max_us) {
        ++g_event_prof.epoll_long_waits;
      } else {
        ++g_event_prof.epoll_short_waits;
      }
    }
    int n = epoll_wait_spin_then_block(epfd, events.data(), static_cast<int>(events.size()),
                                       idle_us);
    record_epoll_batch(n);
    if (n < 0) {
      if (errno == EINTR) {
        poll_tcp_self_warm_child();
        if (g_stop) break;
        continue;
      }
      std::perror("epoll_wait");
      break;
    }
    if (n > 0) {
      if (adaptive_epoll_idle) {
        last_epoll_activity_ns = now_ns();
      }
      consecutive_epoll_timeouts = 0;
      epoll_short_waits_remaining = g_epoll_timeout_backoff_short_waits;
    } else if (n == 0 && timeout_backoff_epoll_idle) {
      ++consecutive_epoll_timeouts;
      if (epoll_short_waits_remaining > 0) {
        --epoll_short_waits_remaining;
      }
    }
    poll_tcp_self_warm_child();

    for (int i = 0; i < n; ++i) {
      uint64_t event_id = events[i].data.u64;
      uint32_t ev = events[i].events;

      if (event_id == kListenEventId) {
        if (g_event_profile) ++g_event_prof.listen_events;
        for (;;) {
          int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break;
          }

          if (g_fd_receiver) {
            add_control(cfd);
            continue;
          }

          add_client(cfd, !(socket_path && socket_path[0] != '\0'));
        }
        continue;
      }

      int control_slot = control_slot_from_event_id(event_id);
      if (control_slot >= 0) {
        if (g_event_profile) ++g_event_prof.control_events;
        int control_fd = control_fds[static_cast<size_t>(control_slot)];
        if (control_fd < 0) continue;
        if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
          close_control_slot(control_slot);
          continue;
        }
        uint64_t control_loop_iters = 0;
        int control_passed_total = 0;
        RecvFdStatus control_last_status = RecvFdStatus::Empty;
        for (;;) {
          ++control_loop_iters;
          if (g_event_profile) ++g_event_prof.control_recv_calls;
          int passed_fds[kMaxControlFdBatch];
          int passed_count = 0;
          RecvFdStatus status = recv_passed_fds(control_fd, passed_fds, kMaxControlFdBatch, passed_count);
          control_last_status = status;
          if (status == RecvFdStatus::Got) {
            if (g_event_profile) ++g_event_prof.control_got;
            control_passed_total += passed_count;
            for (int j = 0; j < passed_count; ++j) {
              add_client(passed_fds[j], true);
            }
            if (g_fd_control_single_recv) break;
            continue;
          }
          if (status == RecvFdStatus::Empty) {
            if (g_event_profile) ++g_event_prof.control_empty;
            break;
          }
          if (g_event_profile) ++g_event_prof.control_errors;
          close_control_slot(control_slot);
          break;
        }
        if (g_event_profile) {
          g_event_prof.control_loop_iters += control_loop_iters;
          if (control_loop_iters > g_event_prof.control_loop_max_iters) {
            g_event_prof.control_loop_max_iters = control_loop_iters;
          }
          if (control_loop_iters > 5) ++g_event_prof.control_loop_over5;
        }
        if (UNLIKELY(g_control_loop_warn && control_loop_iters > 5)) {
          std::fprintf(stderr,
                       "[ALERTA CPU] control loop iters=%llu passed=%d slot=%d last_status=%d\n",
                       static_cast<unsigned long long>(control_loop_iters),
                       control_passed_total, control_slot,
                       static_cast<int>(control_last_status));
        }
        continue;
      }

      int slot = static_cast<int>((event_id & 0xffffffffu) - 1u);
      if (g_event_profile) ++g_event_prof.client_events;
      if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) continue;
      Connection& conn = conns[static_cast<size_t>(slot)];
      if (conn.generation != static_cast<uint32_t>(event_id >> 32)) continue;
      if (!conn.active) continue;
      int fd = conn.fd;
      const uint64_t client_tail_t0 = g_tail_profile ? now_ns() : 0;

      if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        release_connection(epfd, slot, conns, free_slots);
        goto next_event;
      }

      if (ev & EPOLLIN) {
        bool flushed_in_input = false;
        InputStatus input;
        if (g_fd_client_level_trigger) {
          compact_input(conn);
          input = read_input_once(fd, conn);
        } else {
          input = drain_input(fd, conn);
        }
        if (input == InputStatus::Closed) {
          release_connection(epfd, slot, conns, free_slots);
          goto next_event;
        }

        if (input != InputStatus::BadRequest) {
          parse_available_requests(conn);
        }

        if (conn.out_sent < conn.out_used) {
          FlushStatus flushed = flush_output(epfd, slot, conn, conns, free_slots);
          flushed_in_input = true;
          if (flushed == FlushStatus::Closed) goto next_event;
          if (flushed == FlushStatus::Pending) {
            if (!(ev & EPOLLOUT)) mod_epoll(epfd, fd, conn, client_readwrite_events(), event_id);
          } else if (UNLIKELY(g_post_flush_read_spins > 0)) {
            FlushStatus post_flush =
                try_post_flush_read(epfd, slot, conn, conns, free_slots, event_id);
            if (post_flush == FlushStatus::Closed || post_flush == FlushStatus::Pending) {
              goto next_event;
            }
            if (ev & EPOLLOUT) mod_epoll(epfd, fd, conn, client_read_events(), event_id);
          } else if (ev & EPOLLOUT) {
            mod_epoll(epfd, fd, conn, client_read_events(), event_id);
          }
        }

        if (flushed_in_input && (ev & EPOLLOUT)) goto next_event;
      }

      if (ev & EPOLLOUT) {
        FlushStatus flushed = flush_output(epfd, slot, conn, conns, free_slots);
        if (flushed == FlushStatus::Closed) goto next_event;
        if (flushed == FlushStatus::Complete) {
          if (UNLIKELY(g_post_flush_read_spins > 0)) {
            FlushStatus post_flush =
                try_post_flush_read(epfd, slot, conn, conns, free_slots, event_id);
            if (post_flush == FlushStatus::Closed || post_flush == FlushStatus::Pending) {
              goto next_event;
            }
          }
          mod_epoll(epfd, fd, conn, client_read_events(), event_id);
        }
      }

    next_event:
      if (g_tail_profile) {
        const uint64_t elapsed = now_ns() - client_tail_t0;
        ++g_tail.client_events;
        g_tail.client_event_ns += elapsed;
        record_tail_ns(elapsed, g_tail.client_event_bins, g_tail.client_event_max_ns);
      }
      continue;
    }
  }

  for (Connection& conn : conns) {
    if (conn.active) {
      close_fd(conn.fd);
      clear_connection(conn);
    }
  }
  close_all_controls();
  close_fd(epfd);
  close_fd(listen_fd);
  print_profile();
  print_tail_profile();
  print_event_profile();
  print_fragment_profile();
  return 1;
}
