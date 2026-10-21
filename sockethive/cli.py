"""Command-line entry point: ``sockethive {server,client,bench,healthcheck}``."""

from __future__ import annotations

import argparse
import json
import os
import signal
import ssl
import sys
from typing import Any

from . import __version__
from .bench import run_bench
from .client import AuthenticationError, RemoteError, SocketHiveClient
from .config import SERVER_MODES, ServerConfig
from .log import setup_logging
from .security import client_ssl_context
from .server import create_server


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
    sub = parser.add_subparsers(dest="subcommand", required=True)

    srv = sub.add_parser("server", help="run the server (flags override SOCKETHIVE_* env vars)")
    srv.add_argument("--mode", choices=SERVER_MODES, help="concurrency model (default: threaded)")
    srv.add_argument("--host")
    srv.add_argument("--port", type=int)
    srv.add_argument("--max-connections", type=int)
    srv.add_argument("--handler-workers", type=int, help="async mode: handler thread pool size")
    srv.add_argument("--idle-timeout", type=float)
    srv.add_argument("--max-frame-size", type=int)
    srv.add_argument("--shutdown-grace", type=float)
    srv.add_argument("--tls-cert", help="PEM certificate chain; enables TLS with --tls-key")
    srv.add_argument("--tls-key", help="PEM private key")
    srv.add_argument("--handshake-timeout", type=float, help="seconds to finish TLS + auth")
    srv.add_argument(
        "--auth-token",
        help="require this token from clients (prefer SOCKETHIVE_AUTH_TOKEN: "
        "command-line arguments are visible to other local users)",
    )
    srv.add_argument("--rate-limit", type=float, help="requests/second per client IP (0 = off)")
    srv.add_argument("--rate-burst", type=int, help="burst size for --rate-limit")

    def add_target(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", default=os.environ.get("SOCKETHIVE_HOST", "127.0.0.1"))
        p.add_argument("--port", type=int, default=int(os.environ.get("SOCKETHIVE_PORT", 9000)))
        p.add_argument("--timeout", type=float, default=10.0)
        p.add_argument("--tls", action="store_true", help="connect over TLS")
        p.add_argument("--ca-file", help="CA/certificate to trust (default: system store)")
        p.add_argument(
            "--insecure", action="store_true", help="TLS without verifying the server (testing)"
        )
        p.add_argument("--server-name", help="name the server certificate must match")
        p.add_argument(
            "--token",
            default=os.environ.get("SOCKETHIVE_AUTH_TOKEN"),
            help="auth token (env: SOCKETHIVE_AUTH_TOKEN)",
        )

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

    sub.add_parser(
        "healthcheck",
        help="ping the local server using its SOCKETHIVE_* settings (for container probes)",
    )
    return parser


def client_options(args: argparse.Namespace) -> dict[str, Any]:
    """SocketHiveClient keyword arguments for the TLS/auth flags."""
    options: dict[str, Any] = {"auth_token": args.token, "server_hostname": args.server_name}
    if args.tls or args.ca_file or args.insecure:
        options["ssl_context"] = client_ssl_context(args.ca_file, insecure=args.insecure)
    return options


def run_server(args: argparse.Namespace) -> int:
    config = ServerConfig.from_env(
        mode=args.mode,
        host=args.host,
        port=args.port,
        max_connections=args.max_connections,
        handler_workers=args.handler_workers,
        idle_timeout=args.idle_timeout,
        max_frame_size=args.max_frame_size,
        shutdown_grace=args.shutdown_grace,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        handshake_timeout=args.handshake_timeout,
        auth_token=args.auth_token,
        rate_limit=args.rate_limit,
        rate_burst=args.rate_burst,
    )
    server = create_server(config)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: server.request_shutdown())
    server.serve_forever()
    return 0


def run_client(args: argparse.Namespace) -> int:
    client = SocketHiveClient(
        args.host, args.port, timeout=args.timeout, retries=args.retries, **client_options(args)
    )
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
        **client_options(args),
    )
    print(result.report())
    return 1 if result.failed else 0


def run_healthcheck() -> int:
    """Ping the server on loopback with the same settings it was started with."""
    config = ServerConfig.from_env()
    client = SocketHiveClient(
        "127.0.0.1",
        config.port,
        timeout=2.0,
        connect_timeout=2.0,
        retries=0,
        # The certificate is issued for the public name, not 127.0.0.1. This
        # probe only checks liveness, so it encrypts without verifying.
        ssl_context=client_ssl_context(insecure=True) if config.tls_enabled else None,
        auth_token=config.auth_token or None,
    )
    with client:
        client.request("ping")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.log_level)
    try:
        if args.subcommand == "server":
            return run_server(args)
        if args.subcommand == "client":
            return run_client(args)
        if args.subcommand == "healthcheck":
            return run_healthcheck()
        return run_bench_cmd(args)
    except (ConnectionError, AuthenticationError, ssl.SSLError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
