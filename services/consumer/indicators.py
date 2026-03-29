from collections import deque


class RunningEMA:
    def __init__(self, window_size: int) -> None:
        self.window_size = window_size
        self.ema: float | None = None
        self._warmup_prices: list[float] = []
        self.multiplier: float = 2 / (window_size + 1)

    def add(self, price: float) -> float:
        if self.ema is None:
            self._warmup_prices.append(price)
            if len(self._warmup_prices) == self.window_size:
                self.ema = sum(self._warmup_prices) / self.window_size
            return self.ema if self.ema is not None else price
        else:
            self.ema = (price - self.ema) * self.multiplier + self.ema
            return self.ema


class RunningSMA:
    def __init__(self, window_size: int) -> None:
        self.window_size: int = window_size
        self.window: deque[float] = deque(maxlen=window_size)

    def add(self, price: float) -> float:
        self.window.append(price)
        return sum(self.window) / len(self.window)


class RunningRSI:
    def __init__(self, window_size: int):
        self.window_size = window_size
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None
        self.prev_price: float | None = None
        self._changes: list[float] = []

    def add(self, price: float) -> float | None:
        if self.prev_price is None:
            self.prev_price = price
            return None
        change = price - self.prev_price
        self.prev_price = price
        if self.avg_gain is None:
            self._changes.append(change)
            if len(self._changes) == self.window_size:
                self.avg_gain = sum(max(c, 0) for c in self._changes) / self.window_size
                self.avg_loss = (
                    sum(max(-c, 0) for c in self._changes) / self.window_size
                )
                self._changes = []
                return self._rsi()
            return None
        self.avg_gain = (
            self.avg_gain * (self.window_size - 1) + max(change, 0)
        ) / self.window_size
        self.avg_loss = (
            self.avg_loss * (self.window_size - 1) + max(-change, 0)
        ) / self.window_size
        return self._rsi()

    def _rsi(self) -> float:
        if self.avg_loss == 0:
            return 100.0
        rs = self.avg_gain / self.avg_loss
        return 100 - (100 / (1 + rs))
