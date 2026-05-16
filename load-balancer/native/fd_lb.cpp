#include <arpa/inet.h>
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
#include <unistd.h>

namespace {

constexpr int kBacklog = 4096;

void close_fd(int fd) {
  if (fd >= 0) close(fd);
}

int create_tcp_listener(const char* addr_env) {
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
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

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

  return sendmsg(sock, &msg, MSG_NOSIGNAL) == 1;
}

}  // namespace

int main() {
  const char* target1 = std::getenv("FD_TARGET_1");
  const char* target2 = std::getenv("FD_TARGET_2");
  if (!target1 || !target2 || target1[0] == '\0' || target2[0] == '\0') {
    std::fprintf(stderr, "fd lb missing FD_TARGET_1/FD_TARGET_2\n");
    return 1;
  }

  int listen_fd = create_tcp_listener(std::getenv("FD_LB_ADDR"));
  if (listen_fd < 0) {
    std::perror("fd lb listen");
    return 1;
  }

  std::fprintf(stderr, "native fd-passing lb running on %s -> [%s, %s]\n",
               std::getenv("FD_LB_ADDR") ? std::getenv("FD_LB_ADDR") : "0.0.0.0:9999",
               target1, target2);

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

    const char* target = ((rr++ & 1) == 0) ? target1 : target2;
    int ctrl = connect_unix_socket(target);
    if (ctrl < 0 || !send_passed_fd(ctrl, cfd)) {
      close_fd(ctrl);
      close_fd(cfd);
      continue;
    }
    close_fd(ctrl);
    close_fd(cfd);
  }

  close_fd(listen_fd);
  return 1;
}
