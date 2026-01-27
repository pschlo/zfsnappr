from __future__ import annotations
from typing import cast, Optional, TYPE_CHECKING
import logging

from zfsnappr.common.zfs import ZfsProperty
from zfsnappr.common import filter
from zfsnappr.common.utils import get_zfs_cli

from .policy import KeepPolicy
from .prune_snaps import prune_snapshots
from .grouping import GroupType
if TYPE_CHECKING:
  from .args import Args


log = logging.getLogger(__name__)


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

  cli, dataset = get_zfs_cli(args.dataset_spec)
  if dataset is None:
    raise ValueError(f"No dataset specified")

  _all_snaps = cli.get_all_snapshots(dataset=dataset, recursive=args.recursive, sort_by=ZfsProperty.CREATION)
  snapshots = filter.filter_snaps(_all_snaps, tag=filter.parse_tags(args.tag), shortname=filter.parse_shortnames(args.snapshot))
  if not snapshots:
    log.info(f'No matching snapshots, nothing to do')
    return

  get_grouptype: dict[str, Optional[GroupType]] = {
    'dataset': GroupType.DATASET,
    '': None
  }

  prune_snapshots(
    cli,
    snapshots,
    policy,
    dry_run=args.dry_run,
    group_by=get_grouptype[args.group_by],
    allow_destroy_all=bool(args.snapshot)  # only allow if specific snapshots were passed
  )
