from typing import Callable, Optional, Literal
from dataclasses import dataclass
from collections.abc import Collection, Hashable
import string

from .zfs import Snapshot, LocalZfsCli, RemoteZfsCli, ZfsCli


def group_snaps_by[T: Hashable](snapshots: Collection[Snapshot], get_group: Callable[[Snapshot], T]) -> dict[T, list[Snapshot]]:
  groups: dict[T, list[Snapshot]] = {get_group(s): [] for s in snapshots}
  for snap in snapshots:
    groups[get_group(snap)].append(snap)
  return groups


class DatasetParseError(Exception):
  def __init__(self, name: str) -> None:
    super().__init__(f"Invalid dataset name '{name}'")


ALNUM = set(string.ascii_letters + string.digits + '_-')

def is_alnum(value: str):
  return value and set(value) <= ALNUM


@dataclass(frozen=True)
class DatasetConfig:
  user: str | None
  host: str | None
  port: int | None
  dataset: str | None


def parse_dataset(value: str):
  user: str | None
  host: str | None
  port: int | None
  dataset: str | None

  # value = netloc/dataset
  # netloc = user@hostport
  # hostport = host:port
  # value_resolved = user@host:port/dataset

  # split dataset path from domain/netloc
  _parts = value.split('/', maxsplit=1)
  if len(_parts) == 1:
    _netloc, dataset = _parts[0], None
  elif len(_parts) == 2:
    _netloc, dataset = _parts[0] or None, _parts[1] or None
  else:
    assert False

  if _netloc is not None:
    _parts = _netloc.split('@')
    if not all(_parts):
      raise DatasetParseError()
    if len(_parts) == 1:
      user, _hostport = None, _parts[0]
    elif len(_parts) == 2:
      user, _hostport = _parts
    else:
      raise DatasetParseError()
  else:
    user, _hostport = None, None

  if _hostport is not None:
    _parts = _hostport.split(':')
    if not all(_parts):
      raise DatasetParseError()
    if len(_parts) == 1:
      host, port = _parts[0], None
    elif len(_parts) == 2:
      host, port = _parts[0], int(_parts[1])
    else:
      raise DatasetParseError()
  else:
    host, port = None, None

  # Validate
  if not all([
    not user or is_alnum(user),
    not host or is_alnum(host),
    not dataset or all(map(is_alnum, dataset.split('/')[1:]))
  ]):
    raise DatasetParseError()

  return DatasetConfig(
    user=user,
    host=host,
    port=port,
    dataset=dataset
  )


def get_zfs_cli(value: str | None) -> tuple[ZfsCli, str | None]:
  if value is None:
    return LocalZfsCli(), None

  config = parse_dataset(value)
  if config.host:
    cli = RemoteZfsCli(
      host=config.host,
      user=config.user,
      port=config.port
    )
  else:
    cli = LocalZfsCli()

  return cli, config.dataset
