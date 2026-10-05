#!/usr/bin/env python3
"""A disk with a volatile write cache, served over NBD (old-style negotiation), for tests of durability against a loss of power.
   nbdsrv.py IMAGE [port] [seed] [p_persist]
Writes go to a cache that the device may lose; they reach the image only at a FLUSH, with FUA, or by chance at the moment of the cut. SIGUSR1 is the cut: every 4 KB block that is in the cache is written to the image with
probability p_persist (default 0.5: a torn write is possible at every sector, and writes that were issued later can be there while earlier ones are not), the rest is lost (the unit is a block of 4 KB: a disk writes its physical sector whole or not at all), and the server exits. SIGTERM / a disconnect flush and exit.
Reads see the cache (as a real disk does). Prints what it did on stderr."""
import os, signal, socket, struct, sys, random
img, port = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 10809
seed = int(sys.argv[3]) if len(sys.argv) > 3 else 1
p_persist = float(sys.argv[4]) if len(sys.argv) > 4 else 0.5
SEC = 512
fd = os.open(img, os.O_RDWR); size = os.fstat(fd).st_size
cache = {}                          # sector -> bytes (the newest content that is not on the image)
n_flush = n_write = n_read = n_fua = 0

def persist(sectors):
    for s in sorted(sectors):
        os.pwrite(fd, cache[s], s * SEC); del cache[s]
def flush(): persist(list(cache))
def cut(sig, frm):
    rnd = random.Random(seed); blocks = sorted(set(s // 8 for s in cache)); kb = set(b for b in blocks if rnd.random() < p_persist)       # (a physical block of 4 KB is written whole or not at all: the unit of a loss is 4 KB, as on a disk of 4 KB sectors)
    keep = [s for s in sorted(cache) if s // 8 in kb]
    lost = len(cache) - len(keep)
    persist(keep); os.fsync(fd)
    sys.stderr.write("nbdsrv: CUT: %d sectors in the cache, %d reached the image, %d lost (flushes %d, writes %d, fua %d)\n" % (len(keep) + lost, len(keep), lost, n_flush, n_write, n_fua)); sys.stderr.flush()
    os._exit(0)
def term(sig, frm): flush(); os.fsync(fd); os._exit(0)
signal.signal(signal.SIGUSR1, cut); signal.signal(signal.SIGTERM, term)

srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); srv.bind(("127.0.0.1", port)); srv.listen(1)
sys.stderr.write("nbdsrv: ready, %d MB\n" % (size >> 20)); sys.stderr.flush()
c, _ = srv.accept(); c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
FLAGS = 1 | 4 | 8                   # HAS_FLAGS | SEND_FLUSH | SEND_FUA
c.sendall(b"NBDMAGIC" + struct.pack(">QQI", 0x00420281861253, size, FLAGS) + b"\0" * 124)
def recvn(n):
    b = bytearray()
    while len(b) < n:
        d = c.recv(n - len(b))
        if not d: return None
        b += d
    return bytes(b)
def read_range(off, ln):
    out = bytearray(); end = off + ln; s = off // SEC
    while s * SEC < end:
        data = cache.get(s)
        if data is None: data = os.pread(fd, SEC, s * SEC).ljust(SEC, b"\0")
        a = max(off, s * SEC) - s * SEC; b = min(end, (s + 1) * SEC) - s * SEC
        out += data[a:b]; s += 1
    return bytes(out)
def write_range(off, data):
    pos = 0; end = off + len(data); s = off // SEC; touched = []
    while s * SEC < end:
        a = max(off, s * SEC) - s * SEC; b = min(end, (s + 1) * SEC) - s * SEC
        cur = cache.get(s)
        if cur is None: cur = os.pread(fd, SEC, s * SEC).ljust(SEC, b"\0")
        cache[s] = cur[:a] + data[pos:pos + (b - a)] + cur[b:]; pos += b - a; touched.append(s); s += 1
    return touched
while True:
    h = recvn(28)
    if h is None: flush(); break
    magic, ctype, handle, off, ln = struct.unpack(">IHQQI", h[:4] + h[4:6] + h[6:14] + h[14:22] + h[22:26]) if False else (struct.unpack(">I", h[0:4])[0], struct.unpack(">I", h[4:8])[0], h[8:16], struct.unpack(">Q", h[16:24])[0], struct.unpack(">I", h[24:28])[0])
    cmd = ctype & 0xffff; cflags = ctype >> 16
    err = 0; payload = b""
    if cmd == 0: n_read += 1; payload = read_range(off, ln)
    elif cmd == 1:
        data = recvn(ln); t = write_range(off, data); n_write += 1
        if cflags & 1: n_fua += 1; persist([s for s in t if s in cache])
    elif cmd == 2: flush(); break
    elif cmd == 3: n_flush += 1; flush()
    elif cmd == 4: pass
    else: err = 22
    c.sendall(struct.pack(">II", 0x67446698, err) + handle + payload)
os.fsync(fd)
