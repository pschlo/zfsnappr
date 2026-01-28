from collections.abc import Collection

from zfsnappr.common.zfs import Snapshot


def _depth(dataset: str) -> int:
    parts = dataset.split('/')
    assert all(parts)
    return len(parts)


def sort_snaps_by_time(snaps: Collection[Snapshot], reverse: bool = False) -> list[Snapshot]:
    return list(sorted(
        snaps,
        key=lambda s: (s.timestamp, _depth(s.dataset), s.dataset, s.guid),
        reverse=reverse
    ))
