from indicators import RunningEMA


def test_running_ema_warms_up_with_sma_then_updates() -> None:
    ema = RunningEMA(window_size=3)

    assert ema.add(10) == 10
    assert ema.add(12) == 12
    assert ema.add(14) == 12
    assert ema.add(16) == 14

