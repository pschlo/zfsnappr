from __future__ import annotations
from typing import Optional, Callable, cast
from dataclasses import dataclass
import logging

from zfsnappr.common.zfs import Snapshot, Hold, ZfsProperty
from .args import Args
from zfsnappr.common.filter import filter_snaps, parse_tags
from zfsnappr.common.utils import get_zfs_cli


log = logging.getLogger(__name__)

COLUMN_SEPARATOR = ' | '
HEADER_SEPARATOR = '-'

@dataclass
class Field:
  name: str
  get: Callable[[Snapshot], str]

# TODO: Use this list output for other subcommands as well

def entrypoint(args: Args) -> None:
  cli, dataset = get_zfs_cli(args.dataset_spec)

  snaps = cli.get_all_snapshots(dataset=dataset, recursive=args.recursive)
  snaps = filter_snaps(snaps, tag=parse_tags(args.tag))
  snaps = sorted(snaps, key=lambda s: (s.timestamp, s.guid))

  # get hold tags for all snapshots with holds
  holdtags: dict[str, set[str]] = {s.longname: set() for s in snaps}
  for hold in cli.get_holds([s.longname for s in snaps]):
    holdtags[hold.snap_longname].add(hold.tag)

  fields: list[Field] = [
    Field('DATASET',    lambda s: s.dataset),
    Field('SHORT NAME', lambda s: s.shortname),
    Field('TAGS',       lambda s: ','.join(s.tags) if s.tags is not None else 'UNSET'),
    Field('TIMESTAMP',  lambda s: str(s.timestamp)),
    Field('HOLDS',      lambda s: ','.join(holdtags[s.longname]))
  ]
  widths: list[int] = [max(len(f.name), *(len(f.get(s)) for s in snaps), 0) for f in fields]
  total_width = (len(COLUMN_SEPARATOR) * ((len(fields) or 1) - 1)) + sum(widths)

  log.info(COLUMN_SEPARATOR.join(f.name.ljust(w) for f, w in zip(fields, widths)))
  log.info((HEADER_SEPARATOR * (total_width//len(HEADER_SEPARATOR) + 1))[:total_width])
  for snap in snaps:
    log.info(COLUMN_SEPARATOR.join(f.get(snap).ljust(w) for f, w in zip(fields, widths)))
