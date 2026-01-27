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

  _all_snaps = cli.get_all_snapshots(dataset=dataset, recursive=args.recursive, sort_by=ZfsProperty.CREATION)
  snaps = filter_snaps(_all_snaps, shortname=args.snapshot)
  if not snaps:
    log.info(f"No matching snapshots, nothing to do")

  # get hold tags
  _all_holds = cli.get_holds([s.longname for s in snaps])
  release_holds = [h for h in _all_holds if h.tag.startswith('zfsnappr')]
  if not release_holds:
    log.info(f"Snapshots have no releasable holds")

  # Release all zfsnappr holds
  for hold in release_holds:
    log.info(f"Releasing hold '{hold.tag}' on snapshot {hold.snap_longname}")
    cli.release_hold([hold.snap_longname], tag=hold.tag)
