
#include "connection.h"

#include <arpa/inet.h>
#include <cassert>
#include <cstring>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

namespace tcp {

using namespace std;

socket::socket(string servername, int port) {
    sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) throw connection_failure();

    hostent *server;
    server = gethostbyname(servername.c_str());
	if(server == nullptr) throw connection_failure();

    sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);

    while(connect(sock, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        /* do nothing*/;
}
socket::socket(socket &&s) : sock(s.sock) { s.sock = -1; }

socket &socket::operator=(socket &&s) {
    sock = s.sock;
    s.sock = -1;
    return *this;
}

socket::~socket() {
    if(sock >= 0) close(sock);
}

bool socket::read(char *buffer, size_t size) {
    if(sock < 0){
		fprintf(stderr, "WARNING: Attempted to read from closed socket\n");
		return false;
	}
	
    size_t total_bytes = 0;
    while(total_bytes < size) {
        size_t new_bytes =
            ::read(sock, buffer + total_bytes, size - total_bytes);
        if(new_bytes > 0)
            total_bytes += new_bytes;
        else
            return false;
    }
    return true;
}

bool socket::write(char *buffer, size_t size) {
    if (sock < 0) {
		fprintf(stderr, "WARNING: Attempted to write to closed socket\n");
        return false;
    }

    return ::write(sock, buffer, size) == (ssize_t)size;
}

connection_listener::connection_listener(int port) {
    sockaddr_in serv_addr;

    int listenfd = ::socket(AF_INET, SOCK_STREAM, 0);
	if(listenfd < 0) throw connection_failure();

    int reuse_addr = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
               sizeof(reuse_addr));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if(bind(listenfd, (sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        fprintf(stderr, "ERROR on binding\n");
    listen(listenfd, 5);

    fd = unique_ptr<int, std::function<void(int *)>>(
        new int(listenfd), [](int *fd) { close(*fd); });
}

socket connection_listener::accept() {
    int sock = ::accept(*fd, NULL, 0);
	if(sock < 0) throw connection_failure();

    return socket(sock);
}
}
