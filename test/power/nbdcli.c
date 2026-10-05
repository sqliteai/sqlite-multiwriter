// Connects /dev/nbdN to an NBD server (old-style negotiation) with the kernel's ioctls: no nbd-client needed.
//   nbdcli start /dev/nbd0 127.0.0.1 10809     (blocks until the server goes away)      nbdcli stop /dev/nbd0
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <linux/nbd.h>
#include <endian.h>
int main (int argc, char **argv) {
    if (argc >= 3 && !strcmp(argv[1], "stop")) { int d = open(argv[2], O_RDWR); if (d < 0) return 1; ioctl(d, NBD_DISCONNECT); ioctl(d, NBD_CLEAR_SOCK); ioctl(d, NBD_CLEAR_QUE); close(d); return 0; }
    if (argc < 5) { fprintf(stderr, "usage: nbdcli start /dev/nbdN host port | stop /dev/nbdN\n"); return 2; }
    int s = socket(AF_INET, SOCK_STREAM, 0); struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = htons((uint16_t)atoi(argv[4])) }; inet_pton(AF_INET, argv[3], &a.sin_addr);
    int one = 1; setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    for (int i = 0; connect(s, (struct sockaddr *)&a, sizeof a) != 0; i++) { if (i > 100) { perror("connect"); return 1; } usleep(100000); }
    unsigned char hs[152]; size_t got = 0; while (got < sizeof hs) { ssize_t r = read(s, hs + got, sizeof hs - got); if (r <= 0) { perror("handshake"); return 1; } got += (size_t)r; }
    if (memcmp(hs, "NBDMAGIC", 8)) { fprintf(stderr, "not an NBD server\n"); return 1; }
    uint64_t size; uint32_t flags; memcpy(&size, hs + 16, 8); memcpy(&flags, hs + 24, 4); size = be64toh(size); flags = ntohl(flags);
    int d = open(argv[2], O_RDWR); if (d < 0) { perror("open device"); return 1; }
    if (ioctl(d, NBD_SET_BLKSIZE, 4096) < 0 || ioctl(d, NBD_SET_SIZE_BLOCKS, size / 4096) < 0 || ioctl(d, NBD_SET_FLAGS, flags) < 0 || ioctl(d, NBD_SET_TIMEOUT, 60) < 0 || ioctl(d, NBD_SET_SOCK, s) < 0) { perror("ioctl"); return 1; }
    fprintf(stderr, "nbdcli: %s connected (%llu MB)\n", argv[2], (unsigned long long)(size >> 20));
    ioctl(d, NBD_DO_IT);                                                                                 // blocks until the connection ends
    ioctl(d, NBD_CLEAR_QUE); ioctl(d, NBD_CLEAR_SOCK); return 0;
}
