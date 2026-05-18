#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
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

namespace {

bool g_fake_classifier = false;
bool g_cpp_vector_parser = false;
bool g_cpp_ivf = false;
bool g_fd_receiver = false;
bool g_profile = false;
volatile sig_atomic_t g_stop = 0;
NativeIVF g_native_ivf;

struct ProfileCounters {
  uint64_t requests = 0;
  uint64_t body_bytes = 0;
  uint64_t classify_ns = 0;
  uint64_t vector_ns = 0;
  uint64_t bridge_ns = 0;
  uint64_t go_body_ns = 0;
  uint64_t fake_ns = 0;
  uint64_t vector_failures = 0;
};

ProfileCounters g_prof{};

constexpr int kMaxEvents = 2048;
constexpr int kMaxConnections = 4096;
constexpr int kReadBufSize = 8192;
constexpr int kWriteBufSize = 4096;
constexpr int kBacklog = 4096;
constexpr uint32_t kReadEvents = EPOLLIN | EPOLLRDHUP | EPOLLET;
constexpr uint32_t kReadWriteEvents = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
constexpr uint64_t kListenEventId = 0;
constexpr uint64_t kControlEventId = 1;

constexpr std::string_view kGetReadyLine = "GET /ready HTTP/1.1";
constexpr std::string_view kPostFraudLine = "POST /fraud-score HTTP/1.1";
constexpr std::string_view kReadyKeepAlive =
    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
constexpr std::string_view kReadyClose =
    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
constexpr std::string_view k404KeepAlive =
    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
constexpr std::string_view k404Close =
    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
constexpr std::string_view k405KeepAlive =
    "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
constexpr std::string_view k405Close =
    "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
constexpr std::string_view k400Close =
    "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

constexpr std::array<std::string_view, 6> kFraudKeepAlive = {
    "HTTP/1.1 200 OK\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.0}",
    "HTTP/1.1 200 OK\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.2}",
    "HTTP/1.1 200 OK\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.4}",
    "HTTP/1.1 200 OK\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":0.6}",
    "HTTP/1.1 200 OK\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":0.8}",
    "HTTP/1.1 200 OK\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":1.0}",
};

constexpr std::array<std::string_view, 6> kFraudClose = {
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\nConnection: close\r\n\r\n{\"approved\":true,\"fraud_score\":0.0}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\nConnection: close\r\n\r\n{\"approved\":true,\"fraud_score\":0.2}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\nConnection: close\r\n\r\n{\"approved\":true,\"fraud_score\":0.4}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\nConnection: close\r\n\r\n{\"approved\":false,\"fraud_score\":0.6}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\nConnection: close\r\n\r\n{\"approved\":false,\"fraud_score\":0.8}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\nConnection: close\r\n\r\n{\"approved\":false,\"fraud_score\":1.0}",
};

struct Connection {
  int fd = -1;
  std::array<char, kReadBufSize> inbuf{};
  size_t in_start = 0;
  size_t in_end = 0;
  std::array<char, kWriteBufSize> outbuf{};
  size_t out_used = 0;
  size_t out_sent = 0;
  bool close_after_write = false;
  bool active = false;
  uint32_t generation = 0;
};

int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void close_fd(int fd) {
  if (fd >= 0) {
    close(fd);
  }
}

void reset_connection(Connection& conn, int fd) {
  conn.fd = fd;
  conn.in_start = 0;
  conn.in_end = 0;
  conn.out_used = 0;
  conn.out_sent = 0;
  conn.close_after_write = false;
  conn.active = true;
  ++conn.generation;
  if (conn.generation == 0) conn.generation = 1;
}

void clear_connection(Connection& conn) {
  conn.fd = -1;
  conn.in_start = 0;
  conn.in_end = 0;
  conn.out_used = 0;
  conn.out_sent = 0;
  conn.close_after_write = false;
  conn.active = false;
}

uint64_t make_client_event_id(int slot, uint32_t generation) {
  return (static_cast<uint64_t>(generation) << 32) |
         static_cast<uint64_t>(static_cast<uint32_t>(slot + 1));
}

void release_connection(int epfd, int slot, std::vector<Connection>& conns,
                        std::vector<int>& free_slots) {
  if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) return;
  Connection& conn = conns[static_cast<size_t>(slot)];
  int fd = conn.fd;
  if (fd < 0) return;
  epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
  close_fd(fd);
  clear_connection(conn);
  free_slots.push_back(slot);
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
}

bool queue_static_response(Connection& conn, std::string_view resp) {
  if (conn.out_sent < conn.out_used) return false;
  if (resp.size() > conn.outbuf.size()) return false;
  std::memcpy(conn.outbuf.data(), resp.data(), resp.size());
  conn.out_used = resp.size();
  conn.out_sent = 0;
  return true;
}

bool env_equals(const char* value, const char* expected) {
  return value && std::strcmp(value, expected) == 0;
}

void handle_stop(int) {
  g_stop = 1;
}

uint64_t now_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

uint64_t avg_ns(uint64_t total, uint64_t count) {
  return count == 0 ? 0 : total / count;
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
               "total_bridge_ms=%.3f\n",
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
               static_cast<double>(g_prof.bridge_ns) / 1000000.0);
}

int fake_classify(const uint8_t* body, size_t n) {
  if (body == nullptr || n == 0) return -1;
  uint32_t x = static_cast<uint32_t>(n);
  x += static_cast<uint32_t>(body[0]);
  x += static_cast<uint32_t>(body[n >> 1]) << 1;
  x += static_cast<uint32_t>(body[n - 1]) << 2;
  return static_cast<int>(x % kFraudKeepAlive.size());
}

int classify_body(const uint8_t* body, size_t n) {
  if (!g_profile) {
    if (g_fake_classifier) return fake_classify(body, n);
    if (g_cpp_ivf) {
      std::array<float, 16> vector;
      if (!build_fraud_vector_cpp(body, n, vector.data())) return -1;
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
    g_prof.fake_ns += t1 - t0;
    g_prof.classify_ns += t1 - t0;
    return out;
  }
  if (g_cpp_ivf) {
    std::array<float, 16> vector;
    const uint64_t tv0 = now_ns();
    if (!build_fraud_vector_cpp(body, n, vector.data())) {
      const uint64_t tv1 = now_ns();
      ++g_prof.vector_failures;
      g_prof.vector_ns += tv1 - tv0;
      g_prof.classify_ns += tv1 - t0;
      return -1;
    }
    const uint64_t tv1 = now_ns();
    int out = g_native_ivf.classify(vector.data());
    const uint64_t tb1 = now_ns();
    g_prof.vector_ns += tv1 - tv0;
    g_prof.bridge_ns += tb1 - tv1;
    g_prof.classify_ns += tb1 - t0;
    return out;
  }
  if (g_cpp_vector_parser) {
    std::array<float, 16> vector;
    const uint64_t tv0 = now_ns();
    if (!build_fraud_vector_cpp(body, n, vector.data())) {
      const uint64_t tv1 = now_ns();
      ++g_prof.vector_failures;
      g_prof.vector_ns += tv1 - tv0;
      g_prof.classify_ns += tv1 - t0;
      return -1;
    }
    const uint64_t tv1 = now_ns();
    int out = fraud_classify_vector(vector.data());
    const uint64_t tb1 = now_ns();
    g_prof.vector_ns += tv1 - tv0;
    g_prof.bridge_ns += tb1 - tv1;
    g_prof.classify_ns += tb1 - t0;
    return out;
  }
  int out = fraud_classify(const_cast<uint8_t*>(body), n);
  const uint64_t t1 = now_ns();
  g_prof.go_body_ns += t1 - t0;
  g_prof.classify_ns += t1 - t0;
  return out;
}

size_t find_crlf(const char* data, size_t from, size_t limit) {
  for (size_t i = from; i + 1 < limit; ++i) {
    if (data[i] == '\r' && data[i + 1] == '\n') return i;
  }
  return std::string_view::npos;
}

size_t find_headers_end(const char* data, size_t len) {
  for (size_t i = 0; i + 3 < len; ++i) {
    if (data[i] == '\r' && data[i + 1] == '\n' && data[i + 2] == '\r' && data[i + 3] == '\n') return i;
  }
  return std::string_view::npos;
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
  if (len < n + 2) return false;
  if (std::memcmp(data, lit.data(), n) != 0) return false;
  if (data[n] != '\r' || data[n + 1] != '\n') return false;
  req_line_end = n;
  return true;
}

size_t find_lit(const char* data, size_t from, size_t limit, std::string_view needle) {
  if (needle.empty() || limit < from || limit - from < needle.size()) return std::string_view::npos;
  const char first = needle[0];
  for (size_t i = from; i + needle.size() <= limit; ++i) {
    if (data[i] == first && std::memcmp(data + i, needle.data(), needle.size()) == 0) return i;
  }
  return std::string_view::npos;
}

bool fast_content_length(const char* data, size_t from, size_t headers_end, size_t& out) {
  constexpr std::string_view first_key = "Content-Length:";
  if (from + first_key.size() <= headers_end && std::memcmp(data + from, first_key.data(), first_key.size()) == 0) {
    size_t value_start = from + first_key.size();
    size_t value_end = find_crlf(data, value_start, headers_end + 2);
    if (value_end == std::string_view::npos || value_end > headers_end) return false;
    return parse_content_length(data + value_start, value_end - value_start, out);
  }

  constexpr std::string_view key = "\r\nContent-Length:";
  size_t key_pos = find_lit(data, from, headers_end + 2, key);
  if (key_pos == std::string_view::npos) return false;
  size_t value_start = key_pos + key.size();
  size_t value_end = find_crlf(data, value_start, headers_end + 2);
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
  p = find_crlf(data, p + kHost.size(), len);
  if (p == std::string_view::npos) return FastHeaderStatus::Incomplete;
  p += 2;

  if (p + kUserAgent.size() > len) return FastHeaderStatus::Incomplete;
  if (std::memcmp(data + p, kUserAgent.data(), kUserAgent.size()) != 0) return FastHeaderStatus::Fallback;
  p = find_crlf(data, p + kUserAgent.size(), len);
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
    size_t line_end = find_crlf(data, off, len);
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
  const char* data = conn.inbuf.data() + conn.in_start;
  const size_t data_len = conn.in_end - conn.in_start;
  size_t req_line_end = 0;
  if (fixed_request_prefix(data, data_len, kPostFraudLine, req_line_end)) {
    size_t headers_end = 0;
    size_t content_length = 0;
    FastHeaderStatus status = fast_k6_post_headers(data, data_len, req_line_end + 2, headers_end, content_length);
    if (status == FastHeaderStatus::Fallback) {
      status = fast_post_headers(data, data_len, req_line_end + 2, headers_end, content_length);
    }
    if (status == FastHeaderStatus::Incomplete) return false;
    if (status == FastHeaderStatus::Ready) {
      size_t body_start = headers_end + 4;
      size_t total_needed = body_start + content_length;
      if (content_length == 0 || content_length > static_cast<size_t>(kReadBufSize)) {
        clear_output(conn);
        if (!queue_static_response(conn, k400Close)) return false;
        conn.close_after_write = true;
        consumed = data_len;
        return true;
      }
      if (data_len < total_needed) return false;

      consumed = total_needed;
      conn.close_after_write = false;
      auto* body = reinterpret_cast<uint8_t*>(conn.inbuf.data() + conn.in_start + body_start);
      int fraud = classify_body(body, content_length);
      if (fraud < 0) {
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
  if (headers_end == std::string_view::npos) return false;

  req_line_end = 0;
  bool is_get_ready = false;
  bool is_post_fraud = fixed_request_line(data, headers_end, kPostFraudLine, req_line_end);
  if (!is_post_fraud) {
    is_get_ready = fixed_request_line(data, headers_end, kGetReadyLine, req_line_end);
  }
  if (!is_post_fraud && !is_get_ready) {
    req_line_end = find_crlf(data, 0, headers_end + 2);
    if (req_line_end == std::string_view::npos || req_line_end > headers_end) {
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
  bool keep_alive = true;
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
      clear_output(conn);
      if (!queue_static_response(conn, k400Close)) return false;
      conn.close_after_write = true;
      consumed = data_len;
      return true;
    }
  }
  if (data_len < total_needed) return false;

  consumed = total_needed;
  conn.close_after_write = !keep_alive;

  if (is_get_ready) {
    auto resp = static_response(200, keep_alive);
    return queue_static_response(conn, resp);
  }
  if (is_post_fraud) {
    auto* body = reinterpret_cast<uint8_t*>(conn.inbuf.data() + conn.in_start + body_start);
    int fraud = classify_body(body, content_length);
    if (fraud < 0) {
      clear_output(conn);
      if (!queue_static_response(conn, k400Close)) return false;
      conn.close_after_write = true;
      return true;
    }
    const auto& resp = keep_alive ? kFraudKeepAlive[clamp_fraud(fraud)] : kFraudClose[clamp_fraud(fraud)];
    return queue_static_response(conn, resp);
  }

  auto resp = static_response(is_any_get ? 404 : 405, keep_alive);
  return queue_static_response(conn, resp);
}

void compact_input(Connection& conn) {
  if (conn.in_end == conn.inbuf.size() && conn.in_start > 0) {
    size_t remaining = conn.in_end - conn.in_start;
    if (remaining > 0) {
      std::memmove(conn.inbuf.data(), conn.inbuf.data() + conn.in_start, remaining);
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

InputStatus read_input_once(int fd, Connection& conn) {
  compact_input(conn);
  if (conn.in_end == conn.inbuf.size()) {
    queue_bad_request(conn);
    return InputStatus::BadRequest;
  }

  ssize_t r = recv(fd, conn.inbuf.data() + conn.in_end, conn.inbuf.size() - conn.in_end, 0);
  if (r == 0) return InputStatus::Closed;
  if (r < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return InputStatus::Empty;
    return InputStatus::Closed;
  }
  conn.in_end += static_cast<size_t>(r);
  return InputStatus::Data;
}

InputStatus drain_input(int fd, Connection& conn) {
  InputStatus last = InputStatus::Empty;
  for (;;) {
    InputStatus status = read_input_once(fd, conn);
    if (status == InputStatus::Data) {
      last = InputStatus::Data;
      continue;
    }
    if (status == InputStatus::Empty) return last;
    return status;
  }
}

bool parse_available_requests(Connection& conn) {
  bool parsed = false;
  while (true) {
    size_t consumed = 0;
    if (!parse_request(conn, consumed)) break;
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
    if (conn.out_sent < conn.out_used) break;
  }
  return parsed;
}

void mod_epoll(int epfd, int fd, uint32_t events, uint64_t event_id) {
  epoll_event ev{};
  ev.events = events;
  ev.data.u64 = event_id;
  epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

enum class FlushStatus {
  Complete,
  Pending,
  Closed,
};

FlushStatus flush_output(int epfd, int slot, Connection& conn, std::vector<Connection>& conns,
                         std::vector<int>& free_slots) {
  const int fd = conn.fd;
  const size_t remaining = conn.out_used - conn.out_sent;
  ssize_t w = write(fd, conn.outbuf.data() + conn.out_sent, remaining);
  if (w < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return FlushStatus::Pending;
    release_connection(epfd, slot, conns, free_slots);
    return FlushStatus::Closed;
  }
  if (w == 0) return FlushStatus::Pending;
  conn.out_sent += static_cast<size_t>(w);
  if (static_cast<size_t>(w) < remaining) return FlushStatus::Pending;

  clear_output(conn);
  if (conn.close_after_write) {
    release_connection(epfd, slot, conns, free_slots);
    return FlushStatus::Closed;
  }
  return FlushStatus::Complete;
}

int create_unix_listener(const char* socket_path) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return -1;
  if (set_nonblocking(fd) < 0) {
    close_fd(fd);
    return -1;
  }
  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  if (std::strlen(socket_path) >= sizeof(sa.sun_path)) {
    close_fd(fd);
    return -1;
  }
  std::strncpy(sa.sun_path, socket_path, sizeof(sa.sun_path) - 1);
  unlink(socket_path);
  if (bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
    close_fd(fd);
    return -1;
  }
  chmod(socket_path, 0777);
  if (listen(fd, kBacklog) < 0) {
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

RecvFdStatus recv_passed_fd(int sock, int& out_fd) {
  out_fd = -1;
  char byte = 0;
  iovec iov{};
  iov.iov_base = &byte;
  iov.iov_len = 1;

  char control[CMSG_SPACE(sizeof(int))]{};
  msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = sizeof(control);

  ssize_t n;
  for (;;) {
    n = recvmsg(sock, &msg, 0);
    if (n >= 0 || errno != EINTR) break;
  }
  if (n == 0) return RecvFdStatus::Closed;
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return RecvFdStatus::Empty;
    return RecvFdStatus::Error;
  }

  for (cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
    if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS &&
        cmsg->cmsg_len >= CMSG_LEN(sizeof(int))) {
      std::memcpy(&out_fd, CMSG_DATA(cmsg), sizeof(int));
      return RecvFdStatus::Got;
    }
  }
  return RecvFdStatus::Error;
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
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
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

}  // namespace

int main() {
  signal(SIGPIPE, SIG_IGN);
  signal(SIGTERM, handle_stop);
  signal(SIGINT, handle_stop);
  g_fake_classifier = env_equals(std::getenv("NATIVE_CLASSIFIER_MODE"), "fake");
  g_cpp_ivf = env_equals(std::getenv("NATIVE_IVF_MODE"), "cpp");
  g_cpp_vector_parser = env_equals(std::getenv("VECTOR_MODE"), "native") ||
                        env_equals(std::getenv("NATIVE_VECTOR_MODE"), "cpp") ||
                        g_cpp_ivf;
  g_profile = env_equals(std::getenv("NATIVE_PROFILE"), "1");
  const char* fd_control_socket = std::getenv("FD_CONTROL_SOCKET");
  g_fd_receiver = fd_control_socket && fd_control_socket[0] != '\0';
  if (g_cpp_ivf) {
    const char* index_path = std::getenv("NATIVE_INDEX_PATH");
    if (!index_path || index_path[0] == '\0') {
      index_path = "service/index.bin";
    }
    if (!g_native_ivf.load(index_path)) {
      std::fprintf(stderr, "native C++ IVF load failed: %s\n", g_native_ivf.error().c_str());
      return 1;
    }
    g_native_ivf.set_profile(g_profile);
    const uint64_t warm_start = now_ns();
    g_native_ivf.warmup();
    const uint64_t warm_end = now_ns();
    std::fprintf(stderr, "native C++ IVF warmup finished in %.3fms\n",
                 static_cast<double>(warm_end - warm_start) / 1000000.0);
    std::fprintf(stderr, "native C++ IVF enabled\n");
  }
  if (!g_fake_classifier && !g_cpp_ivf && fraud_init() != 0) {
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
    std::fprintf(stderr, "native aggregate profile enabled\n");
  }

  const char* addr = std::getenv("SERVICE_ADDR");
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

  epoll_event listen_ev{};
  listen_ev.events = EPOLLIN | EPOLLET;
  listen_ev.data.u64 = kListenEventId;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &listen_ev) < 0) {
    std::perror("epoll_ctl add listen");
    close_fd(epfd);
    close_fd(listen_fd);
    return 1;
  }

  const char* socket_path = std::getenv("SERVICE_SOCKET");
  std::fprintf(stderr, "native epoll service running on %s\n",
               g_fd_receiver ? fd_control_socket :
               (socket_path && socket_path[0] != '\0' ? socket_path : (addr ? addr : "0.0.0.0:8081")));

  std::vector<Connection> conns(static_cast<size_t>(kMaxConnections));
  std::vector<int> free_slots;
  free_slots.reserve(static_cast<size_t>(kMaxConnections));
  for (int slot = kMaxConnections - 1; slot >= 0; --slot) {
    free_slots.push_back(slot);
  }
  std::array<epoll_event, kMaxEvents> events{};
  int control_fd = -1;

  auto close_control = [&]() {
    if (control_fd >= 0) {
      epoll_ctl(epfd, EPOLL_CTL_DEL, control_fd, nullptr);
      close_fd(control_fd);
      control_fd = -1;
    }
  };

  auto add_control = [&](int cfd) {
    if (set_nonblocking(cfd) < 0) {
      close_fd(cfd);
      return;
    }
    close_control();
    control_fd = cfd;
    epoll_event cev{};
    cev.events = kReadEvents;
    cev.data.u64 = kControlEventId;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, control_fd, &cev) < 0) {
      close_control();
    }
  };

  auto add_client = [&](int cfd, bool is_tcp) {
    if (is_tcp && !g_fd_receiver) {
      int one = 1;
      setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    if (set_nonblocking(cfd) < 0) {
      close_fd(cfd);
      return;
    }
    if (free_slots.empty()) {
      close_fd(cfd);
      return;
    }
    int slot = free_slots.back();
    free_slots.pop_back();
    reset_connection(conns[static_cast<size_t>(slot)], cfd);
    Connection& conn = conns[static_cast<size_t>(slot)];
    epoll_event cev{};
    cev.events = kReadEvents;
    cev.data.u64 = make_client_event_id(slot, conn.generation);
    epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);
  };

  while (!g_stop) {
    int n = epoll_wait(epfd, events.data(), static_cast<int>(events.size()), -1);
    if (n < 0) {
      if (errno == EINTR) {
        if (g_stop) break;
        continue;
      }
      std::perror("epoll_wait");
      break;
    }

    for (int i = 0; i < n; ++i) {
      uint64_t event_id = events[i].data.u64;
      uint32_t ev = events[i].events;

      if (event_id == kListenEventId) {
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

      if (event_id == kControlEventId) {
        if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
          close_control();
          continue;
        }
        for (;;) {
          int passed = -1;
          RecvFdStatus status = recv_passed_fd(control_fd, passed);
          if (status == RecvFdStatus::Got) {
            add_client(passed, true);
            continue;
          }
          if (status == RecvFdStatus::Empty) break;
          close_control();
          break;
        }
        continue;
      }

      int slot = static_cast<int>((event_id & 0xffffffffu) - 1u);
      if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) continue;
      Connection& conn = conns[static_cast<size_t>(slot)];
      if (conn.generation != static_cast<uint32_t>(event_id >> 32)) continue;
      if (!conn.active) continue;
      int fd = conn.fd;

      if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        release_connection(epfd, slot, conns, free_slots);
        continue;
      }

      if (ev & EPOLLIN) {
        InputStatus input = read_input_once(fd, conn);
        if (input == InputStatus::Closed) {
          release_connection(epfd, slot, conns, free_slots);
          goto next_event;
        }

        if (input != InputStatus::BadRequest) {
          bool parsed = parse_available_requests(conn);
          if (!parsed && input == InputStatus::Data &&
              !conn.close_after_write && conn.out_sent >= conn.out_used) {
            input = drain_input(fd, conn);
            if (input == InputStatus::Closed) {
              release_connection(epfd, slot, conns, free_slots);
              goto next_event;
            }
            if (input != InputStatus::BadRequest) {
              parse_available_requests(conn);
            }
          }
        }

        if (conn.out_sent < conn.out_used) {
          FlushStatus flushed = flush_output(epfd, slot, conn, conns, free_slots);
          if (flushed == FlushStatus::Closed) goto next_event;
          if (flushed == FlushStatus::Pending) {
            mod_epoll(epfd, fd, kReadWriteEvents, event_id);
          } else if (ev & EPOLLOUT) {
            mod_epoll(epfd, fd, kReadEvents, event_id);
          }
        }
      }

      if (ev & EPOLLOUT) {
        FlushStatus flushed = flush_output(epfd, slot, conn, conns, free_slots);
        if (flushed == FlushStatus::Closed) goto next_event;
        if (flushed == FlushStatus::Complete) mod_epoll(epfd, fd, kReadEvents, event_id);
      }

    next_event:
      continue;
    }
  }

  for (Connection& conn : conns) {
    if (conn.active) {
      close_fd(conn.fd);
      clear_connection(conn);
    }
  }
  close_control();
  close_fd(epfd);
  close_fd(listen_fd);
  print_profile();
  return 1;
}
