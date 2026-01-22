from __future__ import annotations
from argparse import Namespace
from typing import cast, Optional, TYPE_CHECKING

from zfsnappr.common.zfs import LocalZfsCli, Snapshot, ZfsProperty
from zfsnappr.common import filter

from .policy import KeepPolicy
from .prune_snaps import prune_snapshots
from .grouping import GroupType
if TYPE_CHECKING:
  from .args import Args


def entrypoint(args: Args):
  policy = KeepPolicy(
    last = args.keep_last,
    hourly = args.keep_hourly,
    daily = args.keep_daily,
    weekly = args.keep_weekly,
    monthly = args.keep_monthly,
    yearly = args.keep_yearly,

    within = args.keep_within,
    within_hourly = args.keep_within_hourly,
    within_daily = args.keep_within_daily,
    within_weekly = args.keep_within_weekly,
    within_monthly = args.keep_within_monthly,
    within_yearly = args.keep_within_yearly,

    name = args.keep_name,
    tags = frozenset(args.keep_tag)
  )

  cli = LocalZfsCli()
  snapshots = cli.get_all_snapshots(dataset=args.dataset, recursive=args.recursive, sort_by=ZfsProperty.CREATION)
  snapshots = filter.filter_snaps(snapshots, tag=filter.parse_tags(args.tag))

  get_grouptype: dict[str, Optional[GroupType]] = {
    'dataset': GroupType.DATASET,
    '': None
  }

  prune_snapshots(cli, snapshots, policy, dry_run=args.dry_run, group_by=get_grouptype[args.group_by])
