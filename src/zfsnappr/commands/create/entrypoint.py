from __future__ import annotations
from typing import Optional, cast
import random
import string
import logging

from zfsnappr.common.zfs import ZfsProperty
from zfsnappr.common.utils import get_zfs_cli
from .args import Args


log = logging.getLogger(__name__)


def entrypoint(args: Args) -> None:
  cli, dataset = get_zfs_cli(args.dataset_spec)
  if dataset is None:
    raise ValueError("No dataset specified")
  
  # generate random 10 digit alnum string
  #   10 digit alnum -> (26+26+10)^10 values = 839299365868340224 values = ca. 59.5 bit
  #   ZFS GUID (64 bits) -> 2^64 values = 18446744073709551616 values
  chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
  shortname: str = ''.join(random.choices(chars, k=10))
  fullname = f'{dataset}@{shortname}'

  cli.create_snapshot(
    fullname=fullname,
    recursive=args.recursive,
    properties={
      ZfsProperty.CUSTOM_TAGS: ','.join(args.tag)
    }
  )

  log.info(f'Created snapshot {fullname}')
