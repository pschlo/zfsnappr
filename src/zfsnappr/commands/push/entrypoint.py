from __future__ import annotations
import logging

from zfsnappr.common.replication import replicate
from zfsnappr.common.utils import get_zfs_cli
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
  source_cli, source_dataset = get_zfs_cli(args.dataset_spec)
  if source_dataset is None:
    raise ValueError(f"No dataset specified")

  dest_cli, dest_dataset = get_zfs_cli(args.dest)
  if dest_dataset is None:
    raise ValueError(f"No dest dataset specified")

  log.info(f'Pushing from source dataset "{source_dataset}" to dest dataset "{dest_dataset}"')

  replicate(
    source_cli=source_cli,
    source_dataset=source_dataset,
    dest_cli=dest_cli,
    dest_dataset=dest_dataset,
    recursive=args.recursive,
    initialize=args.init,
    rollback=args.rollback
  )
