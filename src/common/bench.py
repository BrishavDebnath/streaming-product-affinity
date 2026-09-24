"""
Helpers shared by the benchmark scripts (scripts/load_test.py and
scripts/recovery_test.py). Standard library only, so the unit tests can
import them inside the Spark container.
"""

import http.client
import json
import math
import os
import platform
import re
import socket
import time
from datetime import datetime, timezone

REPORT_PATH = os.path.join("docs", "BENCHMARKS.md")

REPORT_HEADER = """# Benchmarks

Measured on a single machine with Docker Compose, using the scripts in
`scripts/`. Re-run them to reproduce the figures. Each run replaces its own
section.

```bash
docker compose run --rm loadtest     # throughput and latency
docker compose run --rm recovery     # crash and restart
```

Kafka, Spark, MongoDB and the load generator all share the same CPU cores, so
these are figures for one laptop, not for a cluster.
"""


# ------------------------------------------------------------------ numbers
def seconds_in(duration):
    """'10 seconds' -> 10.0, '2 minutes' -> 120.0, '1 hour' -> 3600.0."""
    match = re.match(r"\s*([\d.]+)\s*(second|minute|hour)", duration or "")
    if not match:
        return 0.0
    value, unit = float(match.group(1)), match.group(2)
    return value * {"second": 1, "minute": 60, "hour": 3600}[unit]


def percentile(values, pct):
    """Nearest-rank percentile; None for an empty list."""
    data = sorted(v for v in values if v is not None)
    if not data:
        return None
    # ceil, not round(x + 0.5): when pct/100 * n lands on a whole number,
    # round() breaks the .5 tie by parity, which returned rank n for p95 of
    # 20 samples - the maximum, reported as a 95th percentile.
    rank = max(1, min(len(data), math.ceil(pct / 100.0 * len(data))))
    return data[rank - 1]


def rising_floor(samples):
    """
    True when a backlog series is climbing rather than holding steady.

    Each sample is the backlog left after one batch. Compare the lowest value
    of the second half with the highest of the first half: noise cannot make
    the whole second half sit above the whole first half, a real climb does.
    Needs at least four samples; fewer returns None. Callers must drop the
    start of a step, while the backlog is still rising to its steady level.
    """
    values = [v for v in samples if v is not None]
    if len(values) < 4:
        return None
    half = len(values) // 2
    return min(values[-half:]) > max(values[:half])


def keeping_up(lag_samples, drain_seconds, trigger_seconds,
               read_rate=None, sent_rate=None):
    """
    A rate is sustainable when Spark read events as fast as they were sent,
    the backlog did not climb during the step, and what was left was cleared
    within two trigger intervals after the load stopped.
    """
    if drain_seconds is None or drain_seconds > 2 * trigger_seconds + 5:
        return False                    # the backlog outlived the load
    if read_rate is not None and sent_rate and read_rate < 0.9 * sent_rate:
        return False                    # Spark read clearly less than was sent
    rising = rising_floor(lag_samples)
    if rising is None:
        return None                     # too few batches to judge
    return not rising


def to_epoch(value):
    """A datetime from pymongo (naive UTC) or an aware one -> epoch seconds."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


# ------------------------------------------------------------------- report
def environment():
    """What the numbers were measured on, as seen from inside a container."""
    cpu = platform.processor() or ""
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
    except OSError:
        pass
    memory_gb = None
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    memory_gb = round(int(line.split()[1]) / 1024 / 1024, 1)
                    break
    except OSError:
        pass
    return {
        "cpu": cpu or "unknown",
        "cores": os.cpu_count(),
        "memory_gb": memory_gb,
        "measured_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


def update_section(name, body, path=REPORT_PATH):
    """
    Replace the block between <!-- name:start --> and <!-- name:end --> in the
    report, creating the report or the block if needed. Each script owns one
    block, so running one never erases the other's results.
    """
    start, end = f"<!-- {name}:start -->", f"<!-- {name}:end -->"
    block = f"{start}\n{body.rstrip()}\n{end}"
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        text = REPORT_HEADER
    if start in text and end in text:
        before = text.split(start, 1)[0]
        after = text.split(end, 1)[1]
        text = before + block + after
    else:
        text = text.rstrip() + "\n\n" + block + "\n"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    return text


def xychart(title, x_labels, y_label, bars, line=None):
    """
    A Mermaid xychart (GitHub renders it): bars, plus an optional line.
    Values must be numbers; the y axis starts at 0.
    """
    top = max([v for v in bars + (line or []) if v is not None] or [1])
    fmt = ", ".join
    lines = [
        "```mermaid",
        "xychart-beta",
        f'    title "{title}"',
        "    x-axis [" + fmt(f'"{x}"' for x in x_labels) + "]",
        f'    y-axis "{y_label}" 0 --> {int(top * 1.1) + 1}',
        "    bar [" + fmt(str(round(v or 0)) for v in bars) + "]",
    ]
    if line:
        lines.append("    line [" + fmt(str(round(v or 0)) for v in line) + "]")
    lines.append("```")
    return "\n".join(lines)


# ------------------------------------------------------------ Docker Engine
class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path, timeout=30):
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._socket_path)
        self.sock = sock


class DockerEngine:
    """
    The four Docker Engine API calls the recovery test needs, over the Unix
    socket that Compose mounts into the `recovery` container. No docker CLI
    and no extra Python package.
    """

    def __init__(self, socket_path="/var/run/docker.sock"):
        self.socket_path = socket_path

    def _call(self, method, path, expect=(200, 204, 304)):
        conn = _UnixHTTPConnection(self.socket_path)
        try:
            conn.request(method, path, headers={"Host": "docker"})
            resp = conn.getresponse()
            body = resp.read()
        finally:
            conn.close()
        if resp.status not in expect:
            raise RuntimeError(f"Docker API {method} {path} -> {resp.status}: "
                               f"{body[:200]!r}")
        return json.loads(body) if body else None

    def inspect(self, name):
        return self._call("GET", f"/containers/{name}/json")

    def kill(self, name):
        self._call("POST", f"/containers/{name}/kill?signal=SIGKILL")

    def start(self, name):
        self._call("POST", f"/containers/{name}/start")

    def is_running(self, name):
        return bool(self.inspect(name)["State"]["Running"])

    def started_at(self, name):
        """Epoch seconds of the container's last start."""
        raw = self.inspect(name)["State"]["StartedAt"]      # RFC 3339, ns
        match = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?", raw)
        base = datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S")
        frac = float("0" + (match.group(2) or ".0")[:7])
        return base.replace(tzinfo=timezone.utc).timestamp() + frac


def wait_until(predicate, timeout, interval=1.0):
    """Poll `predicate` until it returns something truthy; None on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None
