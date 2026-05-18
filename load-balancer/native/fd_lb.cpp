#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <vector>
#include <time.h>
#include <unistd.h>

namespace {

constexpr int kBacklog = 4096;
int g_defer_accept_seconds = 1;
int g_initial_connect_retries = 3000;
int g_reconnect_retries = 3;
int g_connect_sleep_ms = 10;

struct Backend {
  const char* path = nullptr;
  int ctrl_fd = -1;
};

struct WorkerConfig {
  int listen_fd = -1;
  const char* target1 = nullptr;
  const char* target2 = nullptr;
  std::atomic<uint64_t>* rr = nullptr;
  int worker_id = 0;
  bool local_rr = false;
};

void close_fd(int fd) {
  if (fd >= 0) close(fd);
}

int env_int(const char* name, int fallback, int min_value, int max_value) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  char* end = nullptr;
  long parsed = std::strtol(value, &end, 10);
  if (end == value || parsed < min_value || parsed > max_value) return fallback;
  return static_cast<int>(parsed);
}

bool env_bool(const char* name, bool fallback) {
  const char* value = std::getenv(name);
  if (!value || value[0] == '\0') return fallback;
  return value[0] == '1' || value[0] == 'y' || value[0] == 'Y' ||
         value[0] == 't' || value[0] == 'T';
}

int create_tcp_listener(const char* addr_env, bool reuse_port) {
  std::string addr = addr_env ? addr_env : "0.0.0.0:9999";
  auto pos = addr.rfind(':');
  if (pos == std::string::npos) return -1;
  std::string host = addr.substr(0, pos);
  int port = std::atoi(addr.substr(pos + 1).c_str());
  if (host.empty()) host = "0.0.0.0";

  int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) return -1;
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (reuse_port) {
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
  }
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &g_defer_accept_seconds, sizeof(g_defer_accept_seconds));

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

int connect_unix_socket(const char* socket_path) {
  int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) return -1;

  sockaddr_un sa{};
  sa.sun_family = AF_UNIX;
  if (std::strlen(socket_path) >= sizeof(sa.sun_path)) {
    close_fd(fd);
    return -1;
  }
  std::strncpy(sa.sun_path, socket_path, sizeof(sa.sun_path) - 1);
  if (connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) < 0) {
    close_fd(fd);
    return -1;
  }
  return fd;
}

bool send_passed_fd(int sock, int fd_to_send) {
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

  cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int));
  std::memcpy(CMSG_DATA(cmsg), &fd_to_send, sizeof(int));
  msg.msg_controllen = cmsg->cmsg_len;

  for (;;) {
    ssize_t n = sendmsg(sock, &msg, MSG_NOSIGNAL);
    if (n == 1) return true;
    if (n < 0 && errno == EINTR) continue;
    return false;
  }
}

bool connect_backend(Backend& backend, int retries) {
  for (int i = 0; i < retries; ++i) {
    backend.ctrl_fd = connect_unix_socket(backend.path);
    if (backend.ctrl_fd >= 0) return true;
    if (g_connect_sleep_ms > 0) {
      timespec ts{g_connect_sleep_ms / 1000, (g_connect_sleep_ms % 1000) * 1000 * 1000};
      nanosleep(&ts, nullptr);
    }
  }
  return false;
}

bool send_to_backend(Backend& backend, int cfd) {
  if (send_passed_fd(backend.ctrl_fd, cfd)) return true;

  close_fd(backend.ctrl_fd);
  backend.ctrl_fd = -1;
  if (!connect_backend(backend, g_reconnect_retries)) return false;
  return send_passed_fd(backend.ctrl_fd, cfd);
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

void accept_loop_single(int listen_fd, Backend (&backends)[2]) {
  uint64_t rr = 0;
  for (;;) {
    int cfd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (cfd < 0) {
      if (errno == EINTR) continue;
      std::perror("fd lb accept");
      break;
    }

    int one = 1;
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    Backend& backend = backends[rr++ & 1];
    if (!send_to_backend(backend, cfd)) {
      close_fd(cfd);
      continue;
    }
    close_fd(cfd);
  }
}

void accept_loop_worker(WorkerConfig config) {
  Backend backends[2] = {{config.target1, -1}, {config.target2, -1}};
  if (!connect_all(backends)) {
    std::fprintf(stderr, "fd lb worker %d failed to connect control sockets\n", config.worker_id);
    return;
  }

  uint64_t local_rr = static_cast<uint64_t>(config.worker_id);
  for (;;) {
    int cfd = accept4(config.listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (cfd < 0) {
      if (errno == EINTR) continue;
      std::perror("fd lb accept");
      break;
    }

    int one = 1;
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    uint64_t idx = config.local_rr ? local_rr++ : config.rr->fetch_add(1, std::memory_order_relaxed);
    Backend& backend = backends[idx & 1];
    if (!send_to_backend(backend, cfd)) {
      close_fd(cfd);
      continue;
    }
    close_fd(cfd);
  }

  close_fd(backends[0].ctrl_fd);
  close_fd(backends[1].ctrl_fd);
}

}  // namespace

int main() {
  const char* target1 = std::getenv("FD_TARGET_1");
  const char* target2 = std::getenv("FD_TARGET_2");
  if (!target1 || !target2 || target1[0] == '\0' || target2[0] == '\0') {
    std::fprintf(stderr, "fd lb missing FD_TARGET_1/FD_TARGET_2\n");
    return 1;
  }

  int workers = env_int("FD_LB_WORKERS", 1, 1, 8);
  bool reuse_port = workers > 1 && env_bool("FD_LB_REUSEPORT", false);
  bool local_rr = workers > 1 && env_bool("FD_LB_LOCAL_RR", false);
  g_defer_accept_seconds = env_int("FD_LB_DEFER_ACCEPT_SECONDS", 1, 0, 60);
  g_initial_connect_retries = env_int("FD_LB_CONNECT_RETRIES", 3000, 1, 30000);
  g_reconnect_retries = env_int("FD_LB_RECONNECT_RETRIES", 3, 0, 3000);
  g_connect_sleep_ms = env_int("FD_LB_CONNECT_SLEEP_MS", 10, 0, 1000);

  int listen_fd = create_tcp_listener(std::getenv("FD_LB_ADDR"), reuse_port);
  if (listen_fd < 0) {
    std::perror("fd lb listen");
    return 1;
  }

  Backend backends[2] = {{target1, -1}, {target2, -1}};
  if (workers == 1 && !connect_all(backends)) {
    std::fprintf(stderr, "fd lb failed to connect persistent control sockets\n");
    close_fd(listen_fd);
    return 1;
  }

  std::fprintf(stderr,
               "native fd-passing lb running on %s -> [%s, %s] persistent-control workers=%d reuseport=%d local-rr=%d defer=%d connect-retries=%d reconnect-retries=%d sleep-ms=%d\n",
               std::getenv("FD_LB_ADDR") ? std::getenv("FD_LB_ADDR") : "0.0.0.0:9999",
               target1, target2, workers, reuse_port ? 1 : 0, local_rr ? 1 : 0,
               g_defer_accept_seconds, g_initial_connect_retries, g_reconnect_retries,
               g_connect_sleep_ms);

  if (workers == 1) {
    accept_loop_single(listen_fd, backends);
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
      WorkerConfig config{listen_fds[reuse_port ? i : 0], target1, target2, &rr, i, local_rr};
      threads.emplace_back(accept_loop_worker, config);
    }
    for (auto& thread : threads) {
      thread.join();
    }
    for (size_t i = 1; i < listen_fds.size(); ++i) {
      close_fd(listen_fds[i]);
    }
  }

  close_fd(backends[0].ctrl_fd);
  close_fd(backends[1].ctrl_fd);
  close_fd(listen_fd);
  return 1;
}
