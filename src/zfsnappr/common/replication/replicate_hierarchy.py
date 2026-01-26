from __future__ import annotations
from collections.abc import Collection
import logging

from ..zfs import Snapshot, ZfsCli
from ..utils import group_snaps_by
from .replicate_snaps import replicate_snaps
from zfsnappr.common.exception import ReplicationError


log = logging.getLogger(__name__)


def replicate_hierarchy(
    source_cli: ZfsCli, source_dataset_root: str, source_snaps: Collection[Snapshot],
    dest_cli: ZfsCli, dest_dataset_root: str,
    initialize: bool,
    rollback: bool
):
  """
  replicates given snaps under dest_dataset
  keeps the dataset hierarchy
  all source_snaps must be under source_dataset_root
  """
  is_error: bool = False

  for abs_source_dataset, source_snaps in group_snaps_by(source_snaps, lambda s: s.dataset).items():
    assert abs_source_dataset.startswith(source_dataset_root)
    rel_dataset = abs_source_dataset.removeprefix(source_dataset_root)
    abs_dest_dataset = dest_dataset_root + rel_dataset

    try:
      replicate_snaps(
        source_cli=source_cli,
        source_snaps=source_snaps,
        dest_cli=dest_cli,
        dest_dataset=abs_dest_dataset,
        source_dataset=abs_source_dataset,
        initialize=initialize,
        rollback=rollback
      )
    except ReplicationError as e:
      is_error = True
      log.error(e)

  if is_error:
    raise ReplicationError(f"Replication failed for one or more datasets")
