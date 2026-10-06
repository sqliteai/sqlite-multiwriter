#!/bin/bash
# Durability against a loss of power, without a machine to switch off: the database is on ext4 on an NBD disk whose write cache is volatile (nbdsrv.py), the power of the disk is cut in the middle of a run
# (a random part of the cache, in blocks of 4 KB, reaches the disk, the rest is lost), the file system is mounted again (its journal is replayed), the engine opens the database (its recovery) and mw_serial checks
# every acknowledged transaction is there, and that what is there is a prefix of the commits that is consistent with the model (see test/mw_serial.c).
# Needs a privileged Linux container with an nbd driver, e2fsprogs and python3 (docker build of test/power/Dockerfile) and the repository mounted at /src:
#   MW_MKFS_OPTS='-O ^has_journal': ext4 without a journal (nothing orders a rename before the data of the file: what a missing flush of a directory shows); MW_SEG_MB=1: many segments
#   docker build -t mw-power test/power
#   docker run --rm --privileged -v "$PWD":/src mw-power bash /src/test/power/run.sh [seed] [p_persist] [seconds] [keys]        (MW_SERIAL_MODE=threads: the engine's single-process mode)
set -e
SEED=${1:-1}; P=${2:-0.5}; SECS=${3:-6}; KEYS=${4:-300}
W=/tmp/mwpower_work; rm -rf $W; mkdir -p $W; cd /src; tar cf - --exclude=./build --exclude=./dist --exclude=./.git . | tar xf - -C $W; cd $W
make -j8 dist/mw_serial >/dev/null 2>&1 || { echo "build failed"; exit 2; }
gcc -O1 -o /tmp/nbdcli test/power/nbdcli.c
IMG=/tmp/mwpower_disk.img; rm -f $IMG; truncate -s 1G $IMG
rm -rf /dev/shm/mwp_rec /dev/shm/mwp_side; mkdir -p /dev/shm/mwp_rec /dev/shm/mwp_side
export MW_SERIAL_REBASE=${MW_SERIAL_REBASE:-} MW_SERIAL_MODE=${MW_SERIAL_MODE:-processes} MW_SIDECAR_DIR=/dev/shm/mwp_side MW_SERIAL_REC=/dev/shm/mwp_rec MW_SERIAL_KEYS=$KEYS MW_SERIAL_SECS=$SECS
start_disk () {      # dev
    : > /tmp/mwpower_cli.log
    python3 test/power/nbdsrv.py $IMG 10809 $SEED $P 2>>/tmp/mwpower_srv.log & SRV=$!
    /tmp/nbdcli start $1 127.0.0.1 10809 2>>/tmp/mwpower_cli.log & CLI=$!
    for i in $(seq 1 50); do sleep 0.1; grep -q "connected" /tmp/mwpower_cli.log 2>/dev/null && break; done
}
: > /tmp/mwpower_srv.log; : > /tmp/mwpower_cli.log
echo "== first life: ext4 on a disk with a volatile cache (seed $SEED, a block of the cache survives with probability $P)"
start_disk /dev/nbd0
mkfs.ext4 -q -F ${MW_MKFS_OPTS:-} /dev/nbd0; mkdir -p /mnt/pl; mount /dev/nbd0 /mnt/pl; mkdir -p /mnt/pl/db; sync
SRV1=$SRV; CLI1=$CLI
MW_SERIAL_PHASE=run MW_SERIAL_DB=/mnt/pl/db/t.db MW_SERIAL_CUT_CMD="kill -USR1 $SRV1" ./dist/mw_serial
wait $SRV1 2>/dev/null || true
umount -l /mnt/pl 2>/dev/null || true; /tmp/nbdcli stop /dev/nbd0; wait $CLI1 2>/dev/null || true
grep CUT /tmp/mwpower_srv.log
if [ -n "$MW_MKFS_OPTS" ]; then e2fsck -fy $IMG >/tmp/mwpower_fsck.log 2>&1 || true; tail -3 /tmp/mwpower_fsck.log; fi        # (a file system without a journal is repaired after a loss of power, as it would be at boot)
rm -rf /dev/shm/mwp_side; mkdir -p /dev/shm/mwp_side                                   # (memory does not survive a loss of power: the maps of the shared index are gone)
echo "== second life: the file system is mounted again (journal replay), the database recovers, the check runs"
start_disk /dev/nbd1
mkdir -p /mnt/pl2; mount /dev/nbd1 /mnt/pl2
ls -la /mnt/pl2/db | head -20
set +e
MW_SERIAL_PHASE=verify MW_SERIAL_DB=/mnt/pl2/db/t.db ./dist/mw_serial; RC=$?
set -e
umount /mnt/pl2; /tmp/nbdcli stop /dev/nbd1; kill $SRV 2>/dev/null; wait 2>/dev/null || true
e2fsck -fn $IMG 2>&1 | tail -2
exit $RC
