import pytest

from sockethive.handlers import ClientContext, CommandRegistry, InvalidPayload, default_registry

CTX = ClientContext(client_id=1, address=("127.0.0.1", 5555), stats=lambda: {"active": 1})


@pytest.fixture(scope="module")
def registry():
    return default_registry()


def call(registry, name, payload=None):
    return registry.get(name)(payload, CTX)


def test_basic_commands(registry):
    assert call(registry, "ping") == "pong"
    assert call(registry, "echo", {"a": [1]}) == {"a": [1]}
    assert call(registry, "reverse", "abc") == "cba"
    assert call(registry, "sum", [1, 2, 3.5]) == 6.5
    assert call(registry, "whoami") == {"client_id": 1, "address": ["127.0.0.1", 5555]}
    assert call(registry, "stats") == {"active": 1}
    assert "ping" in call(registry, "commands")


@pytest.mark.parametrize(
    ("name", "payload"),
    [("reverse", 5), ("sum", "nope"), ("sum", [1, "2"]), ("sum", [True]), ("sleep", -1)],
)
def test_invalid_payloads(registry, name, payload):
    with pytest.raises(InvalidPayload):
        call(registry, name, payload)


def test_unknown_command_returns_none(registry):
    assert registry.get("nope") is None


def test_duplicate_registration_rejected():
    reg = CommandRegistry()
    reg.register("x")(lambda p, c: None)
    with pytest.raises(ValueError):
        reg.register("x")(lambda p, c: None)
