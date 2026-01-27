from __future__ import annotations
from typing import Optional, cast
from collections.abc import Collection
import logging
from itertools import pairwise

from ..zfs import Snapshot, ZfsCli, ZfsProperty, Dataset
from .send_receive_snap import send_receive_incremental, send_receive_initial
from zfsnappr.common.exception import ReplicationError


log = logging.getLogger(__name__)


def holdtag_src(dest_dataset: Dataset):
  return f'zfsnappr-sendbase-{dest_dataset.guid}'

def holdtag_dest(src_dataset: Dataset):
  return f'zfsnappr-recvbase-{src_dataset.guid}'


# TODO: raw send for encrypted datasets?
def replicate_snaps(
  source_cli: ZfsCli,
  source_snaps: Collection[Snapshot],
  dest_cli: ZfsCli,
  dest_dataset: str,
  initialize: bool,
  rollback: bool
):
  """
  replicates source_snaps to dest_dataset
  all source_snaps must be of same dataset

  Let S and D be the snapshots on source and dest, newest first.
  Then D[0] = S[b] for some index b.
  We call b the base index. It is used as an incremental basis for sending snapshots S[:b]
  """
  if not source_snaps:
    log.info(f'No source snapshots given, nothing to do')
    return

  source_dataset = next(iter(source_snaps)).dataset

  # sorting is required
  source_snaps = sorted(source_snaps, key=lambda s: s.timestamp, reverse=True)


  ##### PHASE 1: Critical preparation, check for abort conditions

  # ensure dest dataset exists
  dest_exists: bool = any(dest_dataset == d.name for d in dest_cli.get_all_datasets())
  if not dest_exists:
    if initialize:
      log.info(f"Creating destination dataset '{dest_dataset}' by transferring the oldest snapshot")
      source_dataset_type = source_cli.get_dataset(source_dataset).type
      send_receive_initial(
        clis=(source_cli, dest_cli),
        dest_dataset=dest_dataset,
        source_dataset_type=source_dataset_type,
        snapshot=source_snaps[-1],
        holdtags=(holdtag_src, holdtag_dest)
      )
    else:
      raise ReplicationError(f"Destination dataset '{dest_dataset}' does not exist and will not be created")

  # get dest snaps
  dest_snaps = dest_cli.get_all_snapshots(dest_dataset, sort_by=ZfsProperty.CREATION, reverse=True)
  
  # resolve hold tags
  source_tag = holdtag_src(dest_cli.get_dataset(dest_dataset))
  dest_tag = holdtag_dest(source_cli.get_dataset(source_dataset))

  # Clean up obsolete holds
  ensure_holds((source_cli, dest_cli), (source_snaps, dest_snaps), (source_tag, dest_tag))

  if not dest_snaps:
    raise ReplicationError(f"Destination dataset '{dest_dataset}' does not contain any snapshots")

  # figure out base index
  base = next((i for i, s in enumerate(source_snaps) if s.guid == dest_snaps[0].guid), None)
  if base is None:
    raise ReplicationError(f"Latest snapshot '{dest_snaps[0].shortname}' at destination '{dest_dataset}' does not exist on source dataset '{source_dataset}'")


  ##### PHASE 2: Everything technically good to go, do some quality-of-life checks before actual transfer

  if base == 0:
    log.info(f"Source dataset '{source_dataset}' does not have any new snapshots, nothing to do")
    return
  
  # Optionally ensure dest is at snapshot
  if rollback:
    log.info(f"Rolling back destination dataset '{dest_dataset}' to latest snapshot")
    dest_cli.rollback(dest_snaps[0].longname)


  ##### PHASE 3: Transfer snapshots sequentially

  log.info(f"Transferring {base} snapshots from '{source_dataset}' to '{dest_dataset}'")
  for i in range(base):
    send_receive_incremental(
      clis=(source_cli, dest_cli),
      dest_dataset=dest_dataset,
      holdtags=(source_tag, dest_tag),
      snapshot=source_snaps[base-i-1],
      base=source_snaps[base-i],
      unsafe_release=(i > 0)
    )
    log.info(f'{i+1}/{base} transferred')
  dest_snaps = [s.with_dataset(dest_dataset) for s in source_snaps[:base]] + dest_snaps
  log.info(f'Transfer complete')



def ensure_holds(clis: tuple[ZfsCli,ZfsCli], snaps: tuple[list[Snapshot],list[Snapshot]], holdtags: tuple[str,str]):
  """Find the latest snapshot that exists on both sides and ensure it is held. Remove all other peer holdtags.

  After completion, one of these is true:
  1. There are no holdtags on either side, since there was no common snapshot
  2. There is exactly one holdtag on each side, on the latest common snapshot
  """
  # Get holds
  holds = (
    {s.longname: set[str]() for s in snaps[0]},
    {s.longname: set[str]() for s in snaps[1]}
  )
  for h in clis[0].get_holds([s.longname for s in snaps[0]]):
    holds[0][h.snap_longname].add(h.tag)
  for h in clis[1].get_holds([s.longname for s in snaps[1]]):
    holds[1][h.snap_longname].add(h.tag)

  # Find latest common snapshot.
  guid_to_snap = (
    {s.guid: s for s in snaps[0]},
    {s.guid: s for s in snaps[1]}
  )
  common_guids = guid_to_snap[0].keys() & guid_to_snap[1].keys()
  if not common_guids:
    # Release all peer holds.
    release_snaps = (
      [s.longname for s in snaps[0]],
      [s.longname for s in snaps[1]]
    )
    _release_holds(clis, release_snaps, holdtags, current_holdtags=holds)
    return
  # For determinism, sort by GUID if timestamps are equal
  latest_guid = max(common_guids, key=lambda g: (guid_to_snap[0][g].timestamp, g))
  latest_common_snap = (guid_to_snap[0][latest_guid], guid_to_snap[1][latest_guid])
  log.info(f"Latest common snapshot is {latest_common_snap[0].longname} on source, {latest_common_snap[1].longname} on destination")

  # Ensure latest common snap is held
  if holdtags[0] not in holds[0][latest_common_snap[0].longname]:
    clis[0].hold([latest_common_snap[0].longname], tag=holdtags[0])
  if holdtags[1] not in holds[1][latest_common_snap[1].longname]:
    clis[1].hold([latest_common_snap[1].longname], tag=holdtags[1])

  # Remove all other holdtags
  release_snaps = (
    [s.longname for s in snaps[0] if s.guid != latest_common_snap[0].guid],
    [s.longname for s in snaps[1] if s.guid != latest_common_snap[1].guid]
  )
  _release_holds(clis, release_snaps, holdtags, current_holdtags=holds)


def _release_holds(clis: tuple[ZfsCli, ZfsCli], snaps: tuple[list[str], list[str]], release_holdtags: tuple[str, str], current_holdtags: tuple[dict[str, set[str]], dict[str, set[str]]]):
  # Filter for snaps that have the holdtags
  release_snaps = (
    [s for s in snaps[0] if release_holdtags[0] in current_holdtags[0][s]],
    [s for s in snaps[1] if release_holdtags[1] in current_holdtags[1][s]],
  )
  if release_snaps[0]:
    log.info(f"Releasing {len(release_snaps[0])} obsolete holds in source")
  if release_snaps[1]:
    log.info(f"Releasing {len(release_snaps[1])} obsolete holds in destination")
  clis[0].release_hold(release_snaps[0], release_holdtags[0])
  clis[1].release_hold(release_snaps[1], release_holdtags[1])
