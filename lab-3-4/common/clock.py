import time


def now_micros() -> int:
    return time.time_ns() // 1_000
