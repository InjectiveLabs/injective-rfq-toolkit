import base64
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import unquote

import httpx
import pytest

from rfq_test.config import load_environment_config
from rfq_test.crypto.wallet import Wallet


@pytest.fixture
def probe(monkeypatch):
    path = Path(__file__).parents[1] / "scripts" / "probe_taker_auth.py"
    spec = importlib.util.spec_from_file_location("probe_taker_auth", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "get_environment_config", lambda: load_environment_config("testnet"))
    monkeypatch.setattr(
        module, "get_settings", lambda: SimpleNamespace(retail_private_key="11" * 32)
    )
    return module


@pytest.mark.parametrize("active,authenticated", [(True, True), (True, False), (False, False)])
async def test_delegated_probe_checks_scope_and_authentication(probe, monkeypatch, active, authenticated):
    principal = Wallet.from_private_key("22" * 32).inj_address
    delegate = Wallet.from_private_key("11" * 32)
    http = AsyncMock()
    http.__aenter__.return_value = http
    http.get.return_value = httpx.Response(
        200, json={"data": {"active": active}}, request=httpx.Request("GET", "https://lcd.test")
    )
    monkeypatch.setattr(probe.httpx, "AsyncClient", Mock(return_value=http))
    stream = AsyncMock()
    stream.wait_for_auth_result.return_value = {
        "authenticated": authenticated, "code": "", "nonce": "nonce", "message": ""
    }
    factory = Mock(return_value=stream)
    monkeypatch.setattr(probe, "TakerStreamClient", factory)

    assert await probe.main(principal) == (0 if active and authenticated else 1)

    url = http.get.call_args.args[0]
    assert "/inj1rektxpyh40cz8qdcgstwgq89lhts6dy849pn6e/smart/" in url
    query = json.loads(base64.b64decode(unquote(url.rsplit("/", 1)[1])))
    assert query == {"get_authorization_state": {
        "principal": principal,
        "delegate": delegate.inj_address,
        "scope": {"consumer": load_environment_config("testnet").contract.address, "action_id": 3},
    }}
    if active:
        assert factory.call_args.kwargs["request_address"] == principal
        assert factory.call_args.kwargs["auth_private_key"] == delegate.private_key
        assert factory.call_args.kwargs["auth_contract_address"] == load_environment_config("testnet").contract.address
        stream.close.assert_awaited_once()
    else:
        factory.assert_not_called()


async def test_delegated_probe_requires_configured_key(probe, monkeypatch):
    monkeypatch.setattr(probe, "get_settings", lambda: SimpleNamespace(retail_private_key=None))
    factory = Mock()
    monkeypatch.setattr(probe, "TakerStreamClient", factory)
    assert await probe.main(Wallet.from_private_key("22" * 32).inj_address) == 1
    factory.assert_not_called()
