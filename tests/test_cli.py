import pytest

from sockethive.bench import run_bench
from sockethive.cli import main, parse_payload


@pytest.mark.parametrize(
    ("text", "expected"),
    [(None, None), ("[1, 2]", [1, 2]), ('{"a": 1}', {"a": 1}), ("42", 42), ("hello", "hello")],
)
def test_parse_payload(text, expected):
    assert parse_payload(text) == expected


def test_client_one_shot(server, capsys):
    port = str(server.address[1])
    assert main(["client", "--port", port, "sum", "[1, 2, 3]"]) == 0
    assert capsys.readouterr().out.strip() == "6"
    assert main(["client", "--port", port, "nope"]) == 1
    assert "UNKNOWN_COMMAND" in capsys.readouterr().err


def test_client_unreachable(capsys):
    from .conftest import free_port

    assert main(["client", "--port", str(free_port()), "--retries", "0", "ping"]) == 2
    assert "could not connect" in capsys.readouterr().err


def test_bench(server, capsys):
    result = run_bench("127.0.0.1", server.address[1], clients=5, requests=20)
    assert result.completed == 100
    assert result.failed == 0
    assert 0 < result.percentile(50) <= result.percentile(99)
    assert "throughput" in result.report()
    assert main(["bench", "--port", str(server.address[1]), "-c", "2", "-n", "5"]) == 0


def test_server_flags_parse():
    from sockethive.cli import build_parser

    args = build_parser().parse_args(["server", "--mode", "async", "--handler-workers", "4"])
    assert (args.subcommand, args.mode, args.handler_workers) == ("server", "async", 4)
    assert build_parser().parse_args(["server"]).mode is None  # falls back to env/default
    args = build_parser().parse_args(["server", "--dedup-ttl", "0", "--dedup-max-entries", "5"])
    assert (args.dedup_ttl, args.dedup_max_entries) == (0.0, 5)


def test_client_over_tls_with_token(make_server, tls_files, capsys):
    cert, key = tls_files
    server = make_server(tls_cert=cert, tls_key=key, auth_token="t0k")
    port = str(server.address[1])
    base = ["client", "--port", port, "--retries", "0", "--ca-file", cert]
    assert main([*base, "--token", "t0k", "echo", "secure"]) == 0
    assert capsys.readouterr().out.strip() == "secure"
    assert main([*base, "--token", "bad", "ping"]) == 2
    assert "AUTH_FAILED" in capsys.readouterr().err
    assert main(["client", "--port", port, "--retries", "0", "--tls", "ping"]) == 2  # untrusted


def test_bench_with_tls_and_token(make_server, tls_files, capsys):
    cert, key = tls_files
    server = make_server(tls_cert=cert, tls_key=key, auth_token="t0k")
    argv = ["bench", "--port", str(server.address[1]), "--ca-file", cert, "--token", "t0k"]
    assert main([*argv, "-c", "3", "-n", "5"]) == 0
    assert "completed          15" in capsys.readouterr().out


def test_healthcheck_uses_server_env(make_server, tls_files, monkeypatch):
    cert, key = tls_files
    server = make_server(tls_cert=cert, tls_key=key, auth_token="t0k")
    monkeypatch.setenv("SOCKETHIVE_PORT", str(server.address[1]))
    monkeypatch.setenv("SOCKETHIVE_TLS_CERT", cert)
    monkeypatch.setenv("SOCKETHIVE_TLS_KEY", key)
    monkeypatch.setenv("SOCKETHIVE_AUTH_TOKEN", "t0k")
    assert main(["healthcheck"]) == 0
    monkeypatch.setenv("SOCKETHIVE_AUTH_TOKEN", "wrong")
    assert main(["healthcheck"]) == 2
