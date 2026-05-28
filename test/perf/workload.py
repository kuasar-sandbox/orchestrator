# Agent-intermittent workload for sandbox resource-control perf/e2e.
#
# Modes (env WL_MODE):
#   cycles  — deterministic grow/rest cycles (good for reproducible perf)
#   pareto  — Pareto-distributed active durations + Poisson idle gaps
#             (closer to real agent traffic; needs longer windows)
#   idle    — boot, then sleep (for density baselines)
#
# Common env:
#   WL_DURATION   total run time in seconds
#   WL_RMIN_MIB   active phase minimum RSS to hold
#   WL_RMAX_MIB   active phase maximum RSS to hold
#   WL_SEED       deterministic seed
#
# cycles-mode env:
#   WL_CYCLES     number of grow/rest cycles within DURATION
#
# pareto-mode env:
#   WL_LAMBDA     Poisson rate of idle→active transitions (events/s)
#   WL_ALPHA      Pareto shape of active durations (default 1.5)
#   WL_XMIN       Pareto x_min seconds (default 1.0)
#
# Memory pages are dirtied with 0xff in 4 MiB chunks so they stay resident
# (and faulted on the host UFFD) — anonymous pages would otherwise fault
# in lazily without observable RSS.

import math
import os
import random
import sys
import time


def env_int(name, default):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    return int(v)


def env_float(name, default):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    return float(v)


def env_str(name, default):
    return os.environ.get(name, default)


def grow_to(target_bytes, end):
    held = bytearray()
    chunk = 4 * 1024 * 1024
    while len(held) < target_bytes and time.time() < end:
        held.extend(b"\xff" * chunk)
        time.sleep(0.05)
    return held


def run_cycles(duration, rmin, rmax, cycles):
    end = time.time() + duration
    slot = max(1.0, duration / float(cycles))
    print(f"workload mode=cycles dur={duration}s rmin={rmin} rmax={rmax} cycles={cycles}", flush=True)
    for i in range(cycles):
        if time.time() >= end:
            break
        r = random.randint(rmin, rmax)
        target = r * 1024 * 1024
        print(f"cycle {i}: active r={r}MiB", flush=True)
        held = grow_to(target, end)
        rest = max(0.5, slot - 1.0)
        rest = min(rest, max(0.1, end - time.time()))
        if rest > 0:
            time.sleep(rest)
        print(f"cycle {i}: free rss={len(held)//(1024*1024)}MiB", flush=True)
        held = bytearray()


def run_pareto(duration, rmin, rmax, lam, alpha, xmin):
    end = time.time() + duration
    print(f"workload mode=pareto dur={duration}s rmin={rmin} rmax={rmax} lam={lam} alpha={alpha} xmin={xmin}", flush=True)
    next_active = time.time() + max(0.001, -math.log(1 - random.random()) / lam)
    while time.time() < end:
        now = time.time()
        if now < next_active:
            time.sleep(min(0.1, next_active - now))
            continue
        r = random.randint(rmin, rmax)
        target = r * 1024 * 1024
        print(f"active r={r}MiB", flush=True)
        held = grow_to(target, end)
        u = random.random() or 1e-9
        d = xmin / (u ** (1.0 / alpha))
        d = min(d, max(0.1, end - time.time()))
        if d > 0:
            time.sleep(d)
        print(f"idle rss=0 (released {r}MiB)", flush=True)
        held = bytearray()
        next_active = time.time() + max(0.001, -math.log(1 - random.random()) / lam)


def run_idle(duration):
    print(f"workload mode=idle dur={duration}s", flush=True)
    time.sleep(duration)


def main():
    seed = env_int("WL_SEED", 42)
    random.seed(seed)
    duration = env_int("WL_DURATION", 20)
    rmin = env_int("WL_RMIN_MIB", 64)
    rmax = env_int("WL_RMAX_MIB", 128)
    mode = env_str("WL_MODE", "cycles")
    if mode == "cycles":
        cycles = env_int("WL_CYCLES", 3)
        run_cycles(duration, rmin, rmax, cycles)
    elif mode == "pareto":
        lam = env_float("WL_LAMBDA", 0.5)
        alpha = env_float("WL_ALPHA", 1.5)
        xmin = env_float("WL_XMIN", 1.0)
        run_pareto(duration, rmin, rmax, lam, alpha, xmin)
    elif mode == "idle":
        run_idle(duration)
    else:
        print(f"workload: unknown WL_MODE={mode}", file=sys.stderr, flush=True)
        sys.exit(2)
    print("workload done", flush=True)


if __name__ == "__main__":
    main()
