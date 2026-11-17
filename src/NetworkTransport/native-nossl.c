// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sched.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

// Normalize Linux's packed epoll_event and OpenSSL's thread-local error result at the ABI.
struct ready_event { uint64_t id; uint32_t events; uint32_t reserved; };
struct tls_result { int status, length, pending, error; uint64_t reason; };
enum { COMPLETE, WANT_READ, WANT_WRITE, CLOSED, PEER_ABORT, FAILED };

int ep_set_cpu(int cpu)
{
    if (cpu < 0) return 0;
    if (cpu >= CPU_SETSIZE) return -EINVAL;
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
    return sched_setaffinity(0, sizeof(set), &set) ? -errno : 0;
}

int ep_create(void)
{
    int fd = epoll_create1(EPOLL_CLOEXEC);
    return fd < 0 ? -errno : fd;
}

// ipv4 is the address in host byte order (127.0.0.1 is 0x7F000001).
int ep_listener(uint32_t ipv4, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) return -errno;
    int one = 1;
    struct sockaddr_in address = { .sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(ipv4) };
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one))
        || setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one))
        || bind(fd, (struct sockaddr *)&address, sizeof(address)) || listen(fd, 4096)) {
        int error = errno;
        close(fd);
        return -error;
    }
    return fd;
}

int ep_accept(int listener)
{
    int fd = accept4(listener, NULL, NULL, SOCK_CLOEXEC | SOCK_NONBLOCK);
    return fd < 0 ? -errno : fd;
}

// Outbound: a blocking connect() that the caller runs off the worker thread, then the descriptor is switched to the nonblocking, no-delay mode accepted sockets use.
int ep_connect(uint32_t ipv4, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -errno;
    struct sockaddr_in address = { .sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(ipv4) };
    int one = 1;
    int flags;
    if (connect(fd, (struct sockaddr *)&address, sizeof(address))
        || setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one))
        || (flags = fcntl(fd, F_GETFL, 0)) < 0
        || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        int error = errno;
        close(fd);
        return -error;
    }
    return fd;
}

int ep_socket_option(int fd, int option, int value)
{
    int level = option == 2 ? SOL_SOCKET : IPPROTO_TCP;
    int name = option == 0 ? TCP_NODELAY : option == 1 ? TCP_CORK : SO_SNDBUF;
    return setsockopt(fd, level, name, &value, sizeof(value)) ? -errno : 0;
}

int ep_shutdown(int fd) { return shutdown(fd, SHUT_RDWR) ? -errno : 0; }
int ep_close(int fd) { return close(fd) ? -errno : 0; }

int ep_recv(int fd, void *buffer, int length)
{
    ssize_t result;
    do { result = recv(fd, buffer, length, 0); } while (result < 0 && errno == EINTR);
    return result < 0 ? -errno : (int)result;
}

int ep_send(int fd, const void *buffer, int length)
{
    ssize_t result;
    do { result = send(fd, buffer, length, MSG_NOSIGNAL); } while (result < 0 && errno == EINTR);
    return result < 0 ? -errno : (int)result;
}

int ep_eventfd(void)
{
    int fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    return fd < 0 ? -errno : fd;
}

int ep_wake(int fd)
{
    uint64_t one = 1;
    ssize_t result;
    do { result = write(fd, &one, sizeof(one)); } while (result < 0 && errno == EINTR);
    return result == sizeof(one) || (result < 0 && errno == EAGAIN) ? 0 : -errno;
}

int ep_drain_wake(int fd)
{
    uint64_t value;
    ssize_t result;
    do { result = read(fd, &value, sizeof(value)); } while (result < 0 && errno == EINTR);
    return result == sizeof(value) || (result < 0 && errno == EAGAIN) ? 0 : -errno;
}

int ep_control(int epollfd, int operation, int fd, uint32_t events, uint64_t id)
{
    struct epoll_event event = { .events = events, .data.u64 = id };
    return epoll_ctl(epollfd, operation, fd, &event) ? -errno : 0;
}

int ep_wait(int epollfd, struct ready_event *output, int capacity, int timeout)
{
    struct epoll_event events[128];
    if (capacity < 1 || capacity > 128) return -EINVAL;
    int count = epoll_wait(epollfd, events, capacity, timeout);
    if (count < 0) return errno == EINTR ? 0 : -errno;
    for (int i = 0; i < count; i++)
        output[i] = (struct ready_event){ events[i].data.u64, events[i].events, 0 };
    return count;
}

// --- TN: TLS exports stubbed out (client-side plaintext evaluation; no OpenSSL dev headers on this box).
// The managed side only calls these when a worker is created with tls=true, which the client path never does.
void *ep_context(const char *cert, const char *key, uint64_t *error) { (void)cert; (void)key; *error = 1; return 0; }
void *ep_session(void *ctx, int fd, uint64_t *error) { (void)ctx; (void)fd; *error = 1; return 0; }
void ep_tls(void *ssl, int operation, void *buffer, int length, struct tls_result *output)
{ (void)ssl; (void)operation; (void)buffer; (void)length; output->status = FAILED; output->length = 0; output->pending = 0; output->error = 0; output->reason = 0; }
void ep_free_session(void *ssl) { (void)ssl; }
void ep_free_context(void *ctx) { (void)ctx; }
const char *ep_version(void) { return "tn-nossl-stub"; }
