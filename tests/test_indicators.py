import pytest
from indicators import RunningEMA, RunningRSI, RunningSMA


def test_running_ema_warms_up_with_sma_then_updates() -> None:
    ema = RunningEMA(window_size=3)

    assert ema.add(10) == 10
    assert ema.add(12) == 12
    assert ema.add(14) == 12
    assert ema.add(16) == 14


def test_running_ema_uses_standard_smoothing_multiplier() -> None:
    ema = RunningEMA(window_size=4)

    assert ema.multiplier == pytest.approx(2 / 5)
    for price in (1, 2, 3, 4):
        ema.add(price)

    # Seeded with SMA(1..4) = 2.5, then 2.5 + (10 - 2.5) * 0.4
    assert ema.add(10) == pytest.approx(5.5)


def test_running_sma_averages_partial_window_during_warmup() -> None:
    sma = RunningSMA(window_size=3)

    assert sma.add(3) == 3
    assert sma.add(5) == 4


def test_running_sma_drops_oldest_price_once_window_is_full() -> None:
    sma = RunningSMA(window_size=3)
    for price in (1, 2, 3):
        sma.add(price)

    assert sma.add(10) == pytest.approx((2 + 3 + 10) / 3)


def test_running_rsi_returns_none_until_window_of_changes_is_seen() -> None:
    rsi = RunningRSI(window_size=3)

    assert rsi.add(10) is None
    assert rsi.add(11) is None
    assert rsi.add(12) is None
    assert rsi.add(11) is not None


def test_running_rsi_seed_and_wilder_smoothing() -> None:
    rsi = RunningRSI(window_size=3)
    for price in (10, 11, 12):
        rsi.add(price)

    # Changes +1, +1, -1 -> avg_gain 2/3, avg_loss 1/3 -> RS 2 -> RSI 66.67
    assert rsi.add(11) == pytest.approx(100 - 100 / 3)

    # Wilder: avg_gain (2/3*2 + 2)/3 = 10/9, avg_loss (1/3*2 + 0)/3 = 2/9 -> RS 5
    assert rsi.add(13) == pytest.approx(100 - 100 / 6)


def test_running_rsi_is_100_when_there_are_no_losses() -> None:
    rsi = RunningRSI(window_size=2)
    for price in (1, 2):
        rsi.add(price)

    assert rsi.add(3) == 100.0
    assert rsi.add(4) == 100.0


def test_running_rsi_is_0_when_there_are_no_gains() -> None:
    rsi = RunningRSI(window_size=2)
    for price in (5, 4):
        rsi.add(price)

    assert rsi.add(3) == pytest.approx(0.0)
