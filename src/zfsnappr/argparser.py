from __future__ import annotations
import argparse

from . import (
  prune as _prune,
  create as _create,
  push as _push,
  pull as _pull,
  list as _list,
  tag as _tag,
  version as _version
)


def get_args() -> argparse.Namespace:
    # Parent parser for global/common options
    common = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
    common.add_argument('-d', '--dataset', type=str, metavar="DATASET", help="asdf")
    common.add_argument('-r', '--recursive', action='store_true', help="foooo")
    common.add_argument('-n', '--dry-run', action='store_true')
    DEFAULTS = dict(
        dataset=None,
        recursive=False,
        dry_run=False
    )

    # create top-level parser
    parser = argparse.ArgumentParser('zfsnappr', parents=[common], formatter_class=CompactHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # create subcommand parsers
    _list.argparser.setup(
        subparsers.add_parser('list', parents=[common])
    )
    _create.argparser.setup(
        subparsers.add_parser('create', parents=[common])
    )
    _prune.argparser.setup(
        subparsers.add_parser('prune', parents=[common])
    )
    _push.argparser.setup(
        subparsers.add_parser('push', parents=[common])
    )
    _pull.argparser.setup(
        subparsers.add_parser('pull', parents=[common])
    )
    _tag.argparser.setup(
        subparsers.add_parser('tag', parents=[common])
    )
    _version.argparser.setup(
        subparsers.add_parser('version')
    )

    # Merge global defaults in
    args = dict(parser.parse_args()._get_kwargs())
    args = DEFAULTS | args

    return argparse.Namespace(**args)


class CompactHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position=40, width=120)
