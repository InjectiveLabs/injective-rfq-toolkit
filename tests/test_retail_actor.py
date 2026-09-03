import pytest

import rfq_test.actors.retail as retail_module
from rfq_test.actors.retail import RetailUser
from rfq_test.crypto.wallet import Wallet
from rfq_test.models.config import ChainConfig, ContractConfig


PRIVATE_KEY = "11" * 32
CONTRACT = "inj19g43wyj843ydkc845dcdea6su4mgfjwnpjz6h5"


def make_retail_user() -> RetailUser:
    return RetailUser(
        wallet=Wallet.from_private_key(PRIVATE_KEY),
        ws_url="wss://rfq.example",
        contract_config=ContractConfig(address=CONTRACT),
        chain_config=ChainConfig(
            grpc_endpoint="grpc.example:443",
            lcd_endpoint="https://lcd.example",
            chain_id="injective-888",
            evm_chain_id=1439,
        ),
    )


@pytest.mark.parametrize("failure_stage", ["connect", "wait"])
async def test_connect_closes_stream_and_preserves_setup_error(monkeypatch, failure_stage):
    setup_error = RuntimeError(f"{failure_stage} failed")

    class FailingTakerStreamClient:
        instance = None

        def __init__(self, *args, **kwargs):
            self.close_calls = 0
            FailingTakerStreamClient.instance = self

        async def connect(self):
            if failure_stage == "connect":
                raise setup_error

        async def wait_for_auth_result(self):
            raise setup_error

        async def close(self):
            self.close_calls += 1

    monkeypatch.setattr(retail_module, "TakerStreamClient", FailingTakerStreamClient)
    retail = make_retail_user()

    with pytest.raises(RuntimeError) as exc_info:
        await retail.connect()

    assert exc_info.value is setup_error
    assert FailingTakerStreamClient.instance.close_calls == 1
    assert retail._ws_client is None


async def test_connect_closes_stream_when_authentication_is_rejected(monkeypatch):
    class RejectedTakerStreamClient:
        instance = None

        def __init__(self, *args, **kwargs):
            self.close_calls = 0
            RejectedTakerStreamClient.instance = self

        async def connect(self):
            pass

        async def wait_for_auth_result(self):
            return {"authenticated": False, "code": "invalid_signature"}

        async def close(self):
            self.close_calls += 1

    monkeypatch.setattr(retail_module, "TakerStreamClient", RejectedTakerStreamClient)
    retail = make_retail_user()

    with pytest.raises(RuntimeError, match="invalid_signature"):
        await retail.connect()

    assert RejectedTakerStreamClient.instance.close_calls == 1
    assert retail._ws_client is None
