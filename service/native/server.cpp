#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "libclassifier.h"

namespace {

constexpr int kMaxEvents = 1024;
constexpr int kMaxConnections = 4096;
constexpr int kMaxTrackedFds = 65536;
constexpr int kReadBufSize = 8192;
constexpr int kWriteBufSize = 4096;
constexpr int kBacklog = 4096;
constexpr uint32_t kReadEvents = EPOLLIN | EPOLLRDHUP | EPOLLET;
constexpr uint32_t kReadWriteEvents = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;

constexpr std::string_view kGetReadyLine = "GET /ready HTTP/1.1";
constexpr std::string_view kPostFraudLine = "POST /fraud-score HTTP/1.1";
constexpr std::string_view kReadyKeepAlive =
    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
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
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\nConnection: keep-alive\r\n\r\n{\"approved\":true,\"fraud_score\":0.0}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\nConnection: keep-alive\r\n\r\n{\"approved\":true,\"fraud_score\":0.2}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\nConnection: keep-alive\r\n\r\n{\"approved\":true,\"fraud_score\":0.4}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\nConnection: keep-alive\r\n\r\n{\"approved\":false,\"fraud_score\":0.6}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\nConnection: keep-alive\r\n\r\n{\"approved\":false,\"fraud_score\":0.8}",
    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\nConnection: keep-alive\r\n\r\n{\"approved\":false,\"fraud_score\":1.0}",
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
  std::array<char, kWriteBufSize> outbuf{};
  size_t in_start = 0;
  size_t in_end = 0;
  size_t out_used = 0;
  size_t out_sent = 0;
  bool close_after_write = false;
  bool active = false;
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

void release_connection(int epfd, int fd, std::vector<Connection>& conns,
                        std::vector<int>& fd_to_slot, std::vector<int>& free_slots) {
  if (fd < 0) return;
  if (static_cast<size_t>(fd) >= fd_to_slot.size()) {
    close_fd(fd);
    return;
  }
  int slot = fd_to_slot[static_cast<size_t>(fd)];
  fd_to_slot[static_cast<size_t>(fd)] = -1;
  epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
  close_fd(fd);
  if (slot >= 0 && static_cast<size_t>(slot) < conns.size()) {
    clear_connection(conns[static_cast<size_t>(slot)]);
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

bool append_static_response(Connection& conn, std::string_view resp) {
  if (conn.out_used + resp.size() > conn.outbuf.size()) return false;
  std::memcpy(conn.outbuf.data() + conn.out_used, resp.data(), resp.size());
  conn.out_used += resp.size();
  return true;
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

bool parse_request(Connection& conn, size_t& consumed) {
  const char* data = conn.inbuf.data() + conn.in_start;
  const size_t data_len = conn.in_end - conn.in_start;
  size_t headers_end = find_headers_end(data, data_len);
  if (headers_end == std::string_view::npos) return false;

  size_t req_line_end = find_crlf(data, 0, headers_end + 2);
  if (req_line_end == std::string_view::npos || req_line_end > headers_end) {
    conn.out_used = 0;
    if (!append_static_response(conn, k400Close)) return false;
    conn.close_after_write = true;
    consumed = data_len;
    return true;
  }

  const bool is_get_ready = line_eq(data, req_line_end, kGetReadyLine);
  const bool is_post_fraud = line_eq(data, req_line_end, kPostFraudLine);
  const bool is_any_get = req_line_end >= 4 && std::memcmp(data, "GET ", 4) == 0;
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
        conn.out_used = 0;
        if (!append_static_response(conn, k400Close)) return false;
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
      conn.out_used = 0;
      if (!append_static_response(conn, k400Close)) return false;
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
    return append_static_response(conn, resp);
  }
  if (is_post_fraud) {
    auto* body = reinterpret_cast<uint8_t*>(conn.inbuf.data() + conn.in_start + body_start);
    int fraud = fraud_classify(body, content_length);
    if (fraud < 0) {
      conn.out_used = 0;
      if (!append_static_response(conn, k400Close)) return false;
      conn.close_after_write = true;
      return true;
    }
    const auto& resp = keep_alive ? kFraudKeepAlive[clamp_fraud(fraud)] : kFraudClose[clamp_fraud(fraud)];
    return append_static_response(conn, resp);
  }

  auto resp = static_response(is_any_get ? 404 : 405, keep_alive);
  return append_static_response(conn, resp);
}

void mod_epoll(int epfd, int fd, uint32_t events) {
  epoll_event ev{};
  ev.events = events;
  ev.data.fd = fd;
  epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

enum class FlushStatus {
  Complete,
  Pending,
  Closed,
};

FlushStatus flush_output(int epfd, int fd, Connection& conn, std::vector<Connection>& conns,
                         std::vector<int>& fd_to_slot, std::vector<int>& free_slots) {
  while (conn.out_sent < conn.out_used) {
    ssize_t w = send(fd, conn.outbuf.data() + conn.out_sent, conn.out_used - conn.out_sent, MSG_NOSIGNAL);
    if (w < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return FlushStatus::Pending;
      release_connection(epfd, fd, conns, fd_to_slot, free_slots);
      return FlushStatus::Closed;
    }
    if (w == 0) return FlushStatus::Pending;
    conn.out_sent += static_cast<size_t>(w);
  }

  conn.out_sent = 0;
  conn.out_used = 0;
  if (conn.close_after_write) {
    release_connection(epfd, fd, conns, fd_to_slot, free_slots);
    return FlushStatus::Closed;
  }
  return FlushStatus::Complete;
}

int create_listener(const char* addr_env) {
  const char* socket_env = std::getenv("SERVICE_SOCKET");
  if (socket_env && socket_env[0] != '\0') {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    if (set_nonblocking(fd) < 0) {
      close_fd(fd);
      return -1;
    }
    sockaddr_un sa{};
    sa.sun_family = AF_UNIX;
    if (std::strlen(socket_env) >= sizeof(sa.sun_path)) {
      close_fd(fd);
      return -1;
    }
    std::strncpy(sa.sun_path, socket_env, sizeof(sa.sun_path) - 1);
    unlink(socket_env);
    if (bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
      close_fd(fd);
      return -1;
    }
    chmod(socket_env, 0777);
    if (listen(fd, kBacklog) < 0) {
      close_fd(fd);
      return -1;
    }
    return fd;
  }

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

}  // namespace

int main() {
  if (fraud_init() != 0) {
    std::fprintf(stderr, "native bridge init failed\n");
    return 1;
  }

  const char* addr = std::getenv("SERVICE_ADDR");
  int listen_fd = create_listener(addr);
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
  listen_ev.data.fd = listen_fd;
  if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &listen_ev) < 0) {
    std::perror("epoll_ctl add listen");
    close_fd(epfd);
    close_fd(listen_fd);
    return 1;
  }

  const char* socket_path = std::getenv("SERVICE_SOCKET");
  std::fprintf(stderr, "native epoll service running on %s\n",
               socket_path && socket_path[0] != '\0' ? socket_path : (addr ? addr : "0.0.0.0:8081"));

  std::vector<Connection> conns(static_cast<size_t>(kMaxConnections));
  std::vector<int> fd_to_slot(static_cast<size_t>(kMaxTrackedFds), -1);
  std::vector<int> free_slots;
  free_slots.reserve(static_cast<size_t>(kMaxConnections));
  for (int slot = kMaxConnections - 1; slot >= 0; --slot) {
    free_slots.push_back(slot);
  }
  std::array<epoll_event, kMaxEvents> events{};

  for (;;) {
    int n = epoll_wait(epfd, events.data(), static_cast<int>(events.size()), -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      std::perror("epoll_wait");
      break;
    }

    for (int i = 0; i < n; ++i) {
      int fd = events[i].data.fd;
      uint32_t ev = events[i].events;

      if (fd == listen_fd) {
        for (;;) {
          int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK);
          if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break;
          }
          int one = 1;
          if (!(socket_path && socket_path[0] != '\0')) {
            setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
          }
          if (static_cast<size_t>(cfd) >= fd_to_slot.size() || free_slots.empty()) {
            close_fd(cfd);
            continue;
          }
          int slot = free_slots.back();
          free_slots.pop_back();
          reset_connection(conns[static_cast<size_t>(slot)], cfd);
          fd_to_slot[static_cast<size_t>(cfd)] = slot;
          epoll_event cev{};
          cev.events = kReadEvents;
          cev.data.fd = cfd;
          epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);
        }
        continue;
      }

      if (fd < 0 || static_cast<size_t>(fd) >= fd_to_slot.size()) continue;
      int slot = fd_to_slot[static_cast<size_t>(fd)];
      if (slot < 0 || static_cast<size_t>(slot) >= conns.size()) continue;
      Connection& conn = conns[static_cast<size_t>(slot)];
      if (!conn.active) continue;

      if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        release_connection(epfd, fd, conns, fd_to_slot, free_slots);
        continue;
      }

      if (ev & EPOLLIN) {
        for (;;) {
          if (conn.in_end == conn.inbuf.size() && conn.in_start > 0) {
            size_t remaining = conn.in_end - conn.in_start;
            if (remaining > 0) {
              std::memmove(conn.inbuf.data(), conn.inbuf.data() + conn.in_start, remaining);
            }
            conn.in_start = 0;
            conn.in_end = remaining;
          }
          if (conn.in_end == conn.inbuf.size()) {
            conn.out_used = 0;
            conn.out_sent = 0;
            append_static_response(conn, k400Close);
            conn.close_after_write = true;
            break;
          }
          ssize_t r = recv(fd, conn.inbuf.data() + conn.in_end, conn.inbuf.size() - conn.in_end, 0);
          if (r == 0) {
            release_connection(epfd, fd, conns, fd_to_slot, free_slots);
            goto next_event;
          }
          if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            release_connection(epfd, fd, conns, fd_to_slot, free_slots);
            goto next_event;
          }
          conn.in_end += static_cast<size_t>(r);
        }

        while (true) {
          size_t consumed = 0;
          if (!parse_request(conn, consumed)) break;
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
          if (conn.out_used != conn.out_sent) break;
        }

        if (conn.out_used > conn.out_sent) {
          FlushStatus flushed = flush_output(epfd, fd, conn, conns, fd_to_slot, free_slots);
          if (flushed == FlushStatus::Closed) goto next_event;
          mod_epoll(epfd, fd, flushed == FlushStatus::Pending ? kReadWriteEvents : kReadEvents);
        }
      }

      if (ev & EPOLLOUT) {
        FlushStatus flushed = flush_output(epfd, fd, conn, conns, fd_to_slot, free_slots);
        if (flushed == FlushStatus::Closed) goto next_event;
        if (flushed == FlushStatus::Complete) mod_epoll(epfd, fd, kReadEvents);
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
  close_fd(epfd);
  close_fd(listen_fd);
  return 1;
}
