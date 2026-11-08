from typing import Any

from pydantic import BaseModel


class RecordBody(BaseModel):
    value: dict[str, Any]
