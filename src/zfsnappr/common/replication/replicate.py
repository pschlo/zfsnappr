from __future__ import annotations

from ..zfs import ZfsCli, ZfsProperty
from .replicate_snaps import replicate_snaps
from .replicate_hierarchy import replicate_hierarchy
from zfsnappr.common.sort import sort_snaps_by_time


def replicate(
  source_cli: ZfsCli,
  source_dataset: str,
  dest_cli: ZfsCli,
  dest_dataset: str,
  recursive: bool=False,
  initialize: bool=False,
  rollback: bool=False
):
  source_snaps = source_cli.get_all_snapshots(source_dataset, recursive=recursive)
  source_snaps = sort_snaps_by_time(source_snaps, reverse=True)

  # Precompute destination datasets that already exist
  existing_dest_datasets = {d.name for d in dest_cli.get_all_datasets()}

  if recursive:
    replicate_hierarchy(
      source_cli,
      source_dataset,
      source_snaps,
      dest_cli,
      dest_dataset,
      existing_dest_datasets=existing_dest_datasets,
      initialize=initialize,
      rollback=rollback
    )
  else:
    replicate_snaps(
      source_cli=source_cli,
      source_snaps=source_snaps,
      dest_cli=dest_cli,
      dest_dataset=dest_dataset,
      existing_dest_datasets=existing_dest_datasets,
      initialize=initialize,
      rollback=rollback,
    )
