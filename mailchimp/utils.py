from typing import Optional


def round_float(value: Optional[float]):
    return round(value, 6) if value else None
