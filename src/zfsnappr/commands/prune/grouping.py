from typing import Optional, Any
from collections.abc import Callable
from enum import Enum

from zfsnappr.common.zfs import Snapshot


class GroupType(Enum):
  DATASET = 'dataset'


GET_GROUP: dict[GroupType, Callable[[Snapshot], str]] = {
  GroupType.DATASET: (lambda s: s.dataset)
}
