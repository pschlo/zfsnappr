from typing import Callable, Optional, Literal
from collections.abc import Collection, Hashable

from .zfs import Snapshot


def group_snaps_by[T: Hashable](snapshots: Collection[Snapshot], get_group: Callable[[Snapshot], T]) -> dict[T, list[Snapshot]]:
  groups: dict[T, list[Snapshot]] = {get_group(s): [] for s in snapshots}
  for snap in snapshots:
    groups[get_group(snap)].append(snap)
  return groups
