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
