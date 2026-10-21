"""Command-line entry point: ``sockethive {server,client,bench}``."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from typing import Any

from . import __version__
from .bench import run_bench
from .client import RemoteError, SocketHiveClient
from .config import ServerConfig
from .log import setup_logging
from .server import SocketHiveServer


def parse_payload(text: str | None) -> Any:
    """Interpret the payload as JSON when possible, otherwise as a plain string."""
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sockethive", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--log-level",
        default=os.environ.get("SOCKETHIVE_LOG_LEVEL", "INFO"),
        help="DEBUG, INFO, WARNING or ERROR (env: SOCKETHIVE_LOG_LEVEL)",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    srv = sub.add_parser("server", help="run the server (flags override SOCKETHIVE_* env vars)")
    srv.add_argument("--host")
    srv.add_argument("--port", type=int)
    srv.add_argument("--max-connections", type=int)
    srv.add_argument("--idle-timeout", type=float)
    srv.add_argument("--max-frame-size", type=int)
    srv.add_argument("--shutdown-grace", type=float)

    def add_target(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=os.environ.get("SOCKETHIVE_HOST", "127.0.0.1"))
        p.add_argument("--port", type=int, default=int(os.environ.get("SOCKETHIVE_PORT", 9000)))
        p.add_argument("--timeout", type=float, default=10.0)

    cli = sub.add_parser("client", help="send one command, or start an interactive shell")
    add_target(cli)
    cli.add_argument("--retries", type=int, default=5)
    cli.add_argument("command", nargs="?", help="command to send; omit for interactive mode")
    cli.add_argument("payload", nargs="?", help="JSON payload (bare strings are allowed)")

    bench = sub.add_parser("bench", help="load-test a running server")
    add_target(bench)
    bench.add_argument("-c", "--clients", type=int, default=10)
    bench.add_argument("-n", "--requests", type=int, default=100, help="requests per client")
    bench.add_argument("--command", default="echo")
    bench.add_argument("--payload", default='"hello"', help="JSON payload")
    return parser


def run_server(args: argparse.Namespace) -> int:
    config = ServerConfig.from_env(
        host=args.host,
        port=args.port,
        max_connections=args.max_connections,
        idle_timeout=args.idle_timeout,
        max_frame_size=args.max_frame_size,
        shutdown_grace=args.shutdown_grace,
    )
    server = SocketHiveServer(config)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: server.request_shutdown())
    server.serve_forever()
    return 0


def run_client(args: argparse.Namespace) -> int:
    client = SocketHiveClient(args.host, args.port, timeout=args.timeout, retries=args.retries)
    with client:
        if args.command:
            return _send(client, args.command, parse_payload(args.payload))
        return _shell(client)


def _send(client: SocketHiveClient, command: str, payload: Any) -> int:
    try:
        result = client.request(command, payload)
    except RemoteError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result)
    return 0


def _shell(client: SocketHiveClient) -> int:
    print(f"connected to {client.host}:{client.port} as client #{client.client_id}")
    print("type a command and optional JSON payload, e.g. 'sum [1, 2, 3]'; 'quit' to exit")
    while True:
        try:
            line = input("sockethive> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line in {"quit", "exit"}:
            return 0
        command, _, rest = line.partition(" ")
        try:
            _send(client, command, parse_payload(rest.strip() or None))
        except (ConnectionError, TimeoutError) as exc:
            print(f"connection problem: {exc}", file=sys.stderr)


def run_bench_cmd(args: argparse.Namespace) -> int:
    result = run_bench(
        args.host,
        args.port,
        clients=args.clients,
        requests=args.requests,
        command=args.command,
        payload=parse_payload(args.payload),
        timeout=args.timeout,
    )
    print(result.report())
    return 1 if result.failed else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level)
    try:
        if args.mode == "server":
            return run_server(args)
        if args.mode == "client":
            return run_client(args)
        return run_bench_cmd(args)
    except ConnectionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
