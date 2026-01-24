from __future__ import annotations
from typing import Protocol


class CommonArgs(Protocol):
  dataset_spec: str | None
  recursive: bool
  dry_run: bool
