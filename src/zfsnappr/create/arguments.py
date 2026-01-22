from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from ..arguments import Args as CommonArgs


class Args(CommonArgs):
  tag: list[str]
