from __future__ import annotations
from typing import Protocol


class CommonArgs(Protocol):
  dataset: str | None
  recursive: bool
  dry_run: bool
