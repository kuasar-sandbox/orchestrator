"""Failure-only journal projection: 800 input lines, at most 120 output lines.

Reserve space for the first Go fatal stack in the captured input. A missing
stack outside that journal window cannot be recovered by this projector.
"""
import itertools
import re
import sys

INPUT_LINES = 800
OUTPUT_LINES = 120

# Existing event diagnostics, excluding raw stack/register dump selectors.
EVENT = re.compile(
    r"memory:|mem_report|sensor:|BudgetAtSnapshot|workload|Cloud Hypervisor|"
    r"runtime.*(ready|exit)|admit rejected|exec: open guest session:|"
    r"reverse-channel: (restore|attach|quiesce)|CH exited code=|"
    r"received (terminated|interrupt)|primary.*(exit|signal)|held-memory|"
    r"RuntimeError:|^runtime:|^Kernel panic|^CPU:|^general protection fault|"
    r"^BUG:|^Oops:|^#PF:"
)
FATAL = re.compile(r"^(SIG[A-Z0-9]+:|fatal error:|panic:)")
FUNCTION = re.compile(
    r"(runtime\.(?:\(\*?[A-Za-z_][A-Za-z_0-9]*\)\.)?"
    r"[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*)\(.*\)"
)
SOURCE = re.compile(
    r"\s+(?:[^\s]*/)?runtime/([A-Za-z_0-9]+\.(?:go|s)):(\d+)"
    r"(?:\s+(\+0x[0-9a-fA-F]+))?(?:\s+.*)?"
)
GOROUTINE = re.compile(r"goroutine (\d+)(?: [^\[\n]*)? \[([^\]]+)\]:")
STATES = frozenset((
    "running", "runnable", "syscall", "waiting", "idle", "IO wait",
    "chan receive", "chan send", "select", "sleep", "semacquire",
    "GC assist marking", "GC sweep wait", "GC scavenge wait", "finalizer wait",
    "force gc (idle)", "sync.Mutex.Lock", "sync.RWMutex.RLock",
))


def project(lines):
    events, fatal = [], []
    started = closed = stack_started = False
    goroutines = 0
    pending = None
    for index, raw in enumerate(itertools.islice(lines, INPUT_LINES)):
        line = raw.rstrip("\r\n")
        # A source line is eligible only immediately after its runtime call.
        if pending is not None:
            source = SOURCE.fullmatch(line)
            frame_index, function = pending
            frame = [function + "([redacted])"]
            if source:
                frame.append("\t" + source[1] + ":" + source[2] +
                             (" " + source[3] if source[3] else ""))
            fatal.append((frame_index, frame))
            pending = None
            if source:
                continue
        marker = FATAL.match(line)
        if marker:
            if stack_started:
                closed = True
            if not closed:
                started = True
                fatal.append((index, [marker[1] + " [redacted]"]))
            continue
        header = GOROUTINE.fullmatch(line)
        if header:
            if started and not closed:
                stack_started = True
                goroutines += 1
                if goroutines > 1:
                    closed = True
                else:
                    state = header[2].split(",", 1)[0]
                    state = state if state in STATES else "redacted"
                    fatal.append((index, [f"goroutine {header[1]} [{state}]:"]))
            continue
        if line == "runtime stack:":
            if started and not closed:
                stack_started = True
                fatal.append((index, [line]))
            continue
        function = FUNCTION.fullmatch(line)
        if function:
            if started and not closed:
                stack_started = True
                pending = (index, function[1])
            continue
        # Stack content must never fall through to substring event matching
        # (e.g. runtime.goexit or a non-runtime call containing "workload").
        if line.startswith(("goroutine ", "created by ")):
            continue
        if line[:1].isspace() or line.startswith("runtime.") or re.fullmatch(r"\S+\(.*\)\s*", line):
            continue
        if EVENT.search(line):
            events.append((index, [line]))
    if pending is not None:
        fatal.append((pending[0], [pending[1] + "([redacted])"]))

    # Keep whole frame/source pairs. Reserve the first fatal block before
    # choosing the tail of ordinary events, then restore journal order.
    selected, count = [], 0
    for entry in fatal:
        if count + len(entry[1]) > OUTPUT_LINES:
            break
        selected.append(entry)
        count += len(entry[1])
    remaining = OUTPUT_LINES - count
    if remaining:
        selected.extend(events[-remaining:])
    return [line for _, group in sorted(selected) for line in group]


if __name__ == "__main__":
    for line in project(sys.stdin):
        print(line)
