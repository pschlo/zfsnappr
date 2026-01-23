from __future__ import annotations
from typing import Any, TypedDict, cast
import argparse

from .common.args import CommonArgs as CommonArgs
from .commands import (
  prune as _prune,
  create as _create,
  push as _push,
  pull as _pull,
  list as _list,
  tag as _tag,
  version as _version
)


class Args(CommonArgs):
    subcommand: str


def get_args() -> Args:
    # Parent parser for global/common options
    common = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
    common.add_argument('-d', '--dataset', type=str, metavar="DATASET")
    common.add_argument('-r', '--recursive', action='store_true')
    common.add_argument('-n', '--dry-run', action='store_true')
    DEFAULTS = dict(
        dataset=None,
        recursive=False,
        dry_run=False
    )

    # create top-level parser
    parser = argparse.ArgumentParser(parents=[common], formatter_class=CompactHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # create subcommand parsers
    _list.args.setup(
        subparsers.add_parser('list', parents=[common])
    )
    _create.args.setup(
        subparsers.add_parser('create', parents=[common])
    )
    _prune.args.setup(
        subparsers.add_parser('prune', parents=[common])
    )
    _push.args.setup(
        subparsers.add_parser('push', parents=[common])
    )
    _pull.args.setup(
        subparsers.add_parser('pull', parents=[common])
    )
    _tag.args.setup(
        subparsers.add_parser('tag', parents=[common])
    )
    _version.args.setup(
        subparsers.add_parser('version')
    )

    # Merge global defaults in
    args = dict(parser.parse_args()._get_kwargs())
    args = DEFAULTS | args

    return cast(Args, argparse.Namespace(**args))


class CompactHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=40, width=120)
