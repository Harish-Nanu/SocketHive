"""Load generator: many concurrent clients hammering one server."""

from __future__ import annotations

import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .client import SocketHiveClient


@dataclass
class BenchResult:
    clients: int
    requests_per_client: int
    duration: float
    latencies: list[float] = field(default_factory=list)
    errors: dict[str, int] = field(default_factory=dict)

    @property
    def completed(self) -> int:
        return len(self.latencies)

    @property
    def failed(self) -> int:
        return sum(self.errors.values())

    @property
    def throughput(self) -> float:
        return self.completed / self.duration if self.duration else 0.0

    def percentile(self, p: int) -> float:
        if not self.latencies:
            return 0.0
        if len(self.latencies) == 1:
            return self.latencies[0]
        return statistics.quantiles(self.latencies, n=100, method="inclusive")[p - 1]

    def report(self) -> str:
        ms = 1000
        lines = [
            f"clients            {self.clients}",
            f"requests/client    {self.requests_per_client}",
            f"completed          {self.completed}",
            f"failed             {self.failed}",
            f"duration           {self.duration:.3f}s",
            f"throughput         {self.throughput:,.0f} req/s",
        ]
        if self.latencies:
            lines += [
                f"latency p50        {self.percentile(50) * ms:.3f} ms",
                f"latency p95        {self.percentile(95) * ms:.3f} ms",
                f"latency p99        {self.percentile(99) * ms:.3f} ms",
                f"latency max        {max(self.latencies) * ms:.3f} ms",
            ]
        for name, count in sorted(self.errors.items()):
            lines.append(f"error {name:<12} {count}")
        return "\n".join(lines)


def run_bench(
    host: str,
    port: int,
    *,
    clients: int = 10,
    requests: int = 100,
    command: str = "echo",
    payload: Any = "hello",
    timeout: float = 10.0,
) -> BenchResult:
    lock = threading.Lock()
    latencies: list[float] = []
    errors: dict[str, int] = {}
    start_gate = threading.Barrier(clients + 1)

    def record_error(exc: Exception) -> None:
        with lock:
            name = type(exc).__name__
            errors[name] = errors.get(name, 0) + 1

    def worker() -> None:
        client = SocketHiveClient(host, port, timeout=timeout)
        local: list[float] = []
        try:
            client.connect()
        except Exception as exc:
            record_error(exc)
            start_gate.wait()
            return
        start_gate.wait()
        try:
            for _ in range(requests):
                t0 = time.perf_counter()
                try:
                    client.request(command, payload)
                except Exception as exc:
                    record_error(exc)
                    continue
                local.append(time.perf_counter() - t0)
        finally:
            client.close()
            with lock:
                latencies.extend(local)

    threads = [threading.Thread(target=worker, name=f"bench-{i}") for i in range(clients)]
    for t in threads:
        t.start()
    start_gate.wait()  # every client is connected (or has failed): start the clock
    t0 = time.perf_counter()
    for t in threads:
        t.join()
    duration = time.perf_counter() - t0

    return BenchResult(clients, requests, duration, latencies, errors)
