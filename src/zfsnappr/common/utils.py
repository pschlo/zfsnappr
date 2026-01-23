from typing import Callable, Optional, Literal
from collections.abc import Collection, Hashable

from .zfs import Snapshot


def group_snaps_by[T: Hashable](snapshots: Collection[Snapshot], get_group: Callable[[Snapshot], T]) -> dict[T, list[Snapshot]]:
  groups: dict[T, list[Snapshot]] = {get_group(s): [] for s in snapshots}
  for snap in snapshots:
    groups[get_group(snap)].append(snap)
  return groups


class DatasetParseError(Exception):
  def __init__(self) -> None:
    super().__init__("Invalid dataset name")


def parse_dataset(value: str):
  user: str | None
  host: str | None
  dataset: str

  _parts = value.split(':')
  if not all(_parts):
    raise DatasetParseError()
  if len(_parts) == 1:
    _netloc, dataset = None, _parts[0]
  elif len(_parts) == 2:
    _netloc, dataset = _parts
  else:
    raise DatasetParseError()

  if _netloc is not None:
    _parts = _netloc.split('@')
    if not all(_parts):
      raise DatasetParseError()
    if len(_parts) == 1:
      user, host = None, _parts[0]
    elif len(_parts) == 2:
      user, host = _parts
    else:
      raise DatasetParseError()
  else:
    user, host = None, None

  return user, host, dataset
