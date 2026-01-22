from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
  remote: str
  port: Optional[int]
  init: bool


def setup(parser: ArgumentParser) -> None:
  parser.add_argument('remote', metavar='USER@HOST:DATASET')
  parser.add_argument('-p', '--port', type=int)
  parser.add_argument('--init', action='store_true')
