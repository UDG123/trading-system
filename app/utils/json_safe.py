from __future__ import annotations

import json
import math
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _safe_float(value: float) -> float | None:
    return value if math.isfinite(value) else None


def to_json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        return _safe_float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return _safe_float(float(obj))
    if is_dataclass(obj):
        return to_json_safe(asdict(obj))
    if isinstance(obj, dict):
        return {str(k): to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_json_safe(v) for v in obj]

    try:
        import numpy as np

        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return _safe_float(float(obj))
        if isinstance(obj, np.ndarray):
            return [to_json_safe(v) for v in obj.tolist()]
    except Exception:
        pass

    try:
        import pandas as pd

        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if obj is pd.NA:
            return None
    except Exception:
        pass

    if hasattr(obj, "model_dump"):
        try:
            return to_json_safe(obj.model_dump())
        except Exception:
            pass

    if hasattr(obj, "dict"):
        try:
            return to_json_safe(obj.dict())
        except Exception:
            pass

    return str(obj)


def dumps_json_safe(obj: Any) -> str:
    return json.dumps(to_json_safe(obj), allow_nan=False, default=str)
