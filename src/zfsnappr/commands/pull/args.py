from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Protocol
from argparse import ArgumentParser

from zfsnappr.common.args import CommonArgs


class Args(CommonArgs):
  source: str
  init: bool
  rollback: bool
  exclude_dataset: list[str]


def setup(parser: ArgumentParser) -> None:
  parser.add_argument('source', metavar='USER@HOST:PORT/DATASET')
  parser.add_argument('--init', action='store_true')
  parser.add_argument('--rollback', action='store_true')
  parser.add_argument('--exclude-dataset', action='append', default=[])
