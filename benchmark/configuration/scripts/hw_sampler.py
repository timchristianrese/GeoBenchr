#!/usr/bin/env python3
"""Hardware usage sampler for the benchmark nodes (2026-10-07).

Every INTERVAL seconds, appends to two CSV files (UTC timestamps, so they line up with the benchmark
client's `at=` log tags):

  <out>/hw_<host>.csv      host-wide: CPU % (user/system/iowait/idle, of all cores), load, memory,
                           swap, disk read/write MB/s, network rx/tx MB/s
  <out>/proc_<host>.csv    per process group (postgres, clickhouse, duckdb, spacetimed, java = Spark/
                           YARN/HDFS, other): CPU % (of one core, so 2000 % = 20 cores busy) and RSS

Only /proc is read, no dependencies. Stops when <out>/STOP exists or after --hours.
    python3 hw_sampler.py --out ~/hw_monitor --interval 5 --hours 24
"""
import argparse
import os
import socket
import time
from datetime import datetime, timezone

GROUPS = [("postgres", "postgres"), ("clickhouse", "clickhouse"), ("duckdb", "duckdb"),
          ("spacetimed", "spacetime"), ("java", "java"), ("minio", "minio")]
CLK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")


def cpu_times():
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:]]
    user, nice, system, idle, iowait, irq, softirq, steal = (v + [0] * 8)[:8]
    return {"user": user + nice, "system": system + irq + softirq, "iowait": iowait, "idle": idle, "steal": steal}


def meminfo():
    m = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            m[k] = int(v.split()[0]) // 1024   # MB
    return m


def disk_bytes():
    rd = wr = 0
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            name = p[2]
            # whole physical disks only (sdX, nvmeXnY, vdX), not partitions or md/dm devices
            if (name.startswith(("sd", "vd")) and not name[-1].isdigit()) or (name.startswith("nvme") and "p" not in name[4:]):
                rd += int(p[5]) * 512
                wr += int(p[9]) * 512
    return rd, wr


def net_bytes():
    rx = tx = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, data = line.split(":", 1)
            if name.strip() == "lo":
                continue
            p = data.split()
            rx += int(p[0])
            tx += int(p[8])
    return rx, tx


def group_of(comm):
    for prefix, g in GROUPS:
        if comm.startswith(prefix):
            return g
    return "other"


def proc_snapshot():
    """pid -> (group, cpu ticks, rss MB)"""
    out = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/stat") as f:
                s = f.read()
            comm = s[s.index("(") + 1:s.rindex(")")]
            rest = s[s.rindex(")") + 2:].split()
            ticks = int(rest[11]) + int(rest[12])
            rss = int(rest[21]) * PAGE // (1024 * 1024)
            out[pid] = (group_of(comm), ticks, rss)
        except (OSError, ValueError, IndexError):
            continue
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.expanduser("~/hw_monitor"))
    ap.add_argument("--interval", type=float, default=5)
    ap.add_argument("--hours", type=float, default=24)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    host = socket.gethostname()
    hw_path, proc_path = f"{a.out}/hw_{host}.csv", f"{a.out}/proc_{host}.csv"
    if not os.path.exists(hw_path):
        open(hw_path, "w").write("ts,host,cpu_user_pct,cpu_system_pct,cpu_iowait_pct,cpu_idle_pct,load1,"
                                 "mem_total_mb,mem_used_mb,mem_available_mb,swap_used_mb,"
                                 "disk_read_mbs,disk_write_mbs,net_rx_mbs,net_tx_mbs\n")
    if not os.path.exists(proc_path):
        open(proc_path, "w").write("ts,host,group,cpu_pct_of_one_core,rss_mb,processes\n")
    end = time.time() + a.hours * 3600
    c0, d0, n0, p0, t0 = cpu_times(), disk_bytes(), net_bytes(), proc_snapshot(), time.time()
    while time.time() < end and not os.path.exists(f"{a.out}/STOP"):
        time.sleep(a.interval)
        c1, d1, n1, p1, t1 = cpu_times(), disk_bytes(), net_bytes(), proc_snapshot(), time.time()
        dt = t1 - t0
        tot = sum(c1[k] - c0[k] for k in c1) or 1
        pct = {k: 100.0 * (c1[k] - c0[k]) / tot for k in c1}
        m = meminfo()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        with open("/proc/loadavg") as f:
            load1 = f.read().split()[0]
        with open(hw_path, "a") as f:
            f.write(f"{ts},{host},{pct['user']:.1f},{pct['system']:.1f},{pct['iowait']:.1f},{pct['idle']:.1f},{load1},"
                    f"{m['MemTotal']},{m['MemTotal'] - m['MemAvailable']},{m['MemAvailable']},{m['SwapTotal'] - m['SwapFree']},"
                    f"{(d1[0] - d0[0]) / dt / 1e6:.2f},{(d1[1] - d0[1]) / dt / 1e6:.2f},"
                    f"{(n1[0] - n0[0]) / dt / 1e6:.2f},{(n1[1] - n0[1]) / dt / 1e6:.2f}\n")
        agg = {}
        for pid, (g, ticks, rss) in p1.items():
            prev = p0.get(pid)
            dticks = ticks - prev[1] if prev and prev[0] == g else 0
            c, r, n = agg.get(g, (0, 0, 0))
            agg[g] = (c + dticks, r + rss, n + 1)
        with open(proc_path, "a") as f:
            for g, (ticks, rss, n) in sorted(agg.items()):
                f.write(f"{ts},{host},{g},{100.0 * ticks / CLK / dt:.1f},{rss},{n}\n")
        c0, d0, n0, p0, t0 = c1, d1, n1, p1, t1


if __name__ == "__main__":
    main()
