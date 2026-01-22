from __future__ import annotations
from argparse import Namespace
from typing import Optional, cast, Literal, Callable
import importlib.metadata
import logging

from .arguments import Args


log = logging.getLogger(__name__)

TAG_SEPARATOR = "_"


def entrypoint(args: Args) -> None:
  version = importlib.metadata.version('zfsnappr')
  log.info(f"zfsnappr {version}")
