from __future__ import annotations
from typing import Protocol, Optional, TypedDict
from dataclasses import dataclass


class Args(Protocol):
  dataset: str | None
  recursive: bool
  dry_run: bool
