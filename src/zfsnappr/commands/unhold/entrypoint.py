from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
import logging

from zfsnappr.common.zfs import Snapshot, Hold, ZfsProperty
from .args import Args
from zfsnappr.common.filter import filter_snaps, parse_tags
from zfsnappr.common import filter
from zfsnappr.common.utils import get_zfs_cli


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
  cli, dataset = get_zfs_cli(args.dataset_spec)
  if dataset is None:
    raise ValueError(f"No dataset specified")

  snaps = cli.get_all_snapshots(dataset=dataset, recursive=args.recursive, sort_by=ZfsProperty.CREATION)
  snaps = filter_snaps(snaps, shortname=args.snapshot)
  if not snaps:
    raise ValueError(f"No matching snapshots")

  # get hold tags
  holds = cli.get_holds([s.longname for s in snaps])

  # Release all zfsnappr holds
  for hold in holds:
    if hold.tag.startswith('zfsnappr'):
      log.info(f"Releasing hold '{hold.tag}' on snapshot {hold.snap_longname}")
      cli.release_hold([hold.snap_longname], tag=hold.tag)
