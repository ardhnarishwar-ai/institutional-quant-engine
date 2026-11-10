from core.risk.policy import DEFAULT_RISK_POLICIES


def test_four_instruments_have_isolated_policy_objects():
    keys = (
        "NSE:NSE_SPOT:NIFTY:INR",
        "NSE:NSE_SPOT:BANKNIFTY:INR",
        "NSE:NSE_SPOT:FINNIFTY:INR",
        "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT",
    )
    assert all(key in DEFAULT_RISK_POLICIES for key in keys)
    assert len({id(DEFAULT_RISK_POLICIES[key]) for key in keys}) == 4
    assert all(DEFAULT_RISK_POLICIES[key].instrument_id == key for key in keys)


def test_policy_weights_are_normalized():
    for policy in DEFAULT_RISK_POLICIES.values():
        assert abs(sum(weight for _, weight in policy.weights) - 1.0) < 1e-9
