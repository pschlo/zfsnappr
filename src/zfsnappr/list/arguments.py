from __future__ import annotations
from dataclasses import dataclass

from ..arguments import Args as CommonArgs


class Args(CommonArgs):
  tag: list[str]
