#!/usr/bin/env python3

from __future__ import annotations

from .setup_logging import setup_logging
setup_logging()
from .argparser import get_args
from . import (
  prune as _prune,
  create as _create,
  push as _push,
  pull as _pull,
  list as _list,
  tag as _tag,
  version as _version
)


def cli():
    args = get_args()
    subcommand = args.subcommand
    args.__delattr__('subcommand')

    s = subcommand
    if s == 'prune':
        _prune.entrypoint(args)
    elif s == 'create':
        _create.entrypoint(args)
    elif s == 'push':
        _push.entrypoint(args)
    elif s == 'pull':
        _pull.entrypoint(args)
    elif s == 'list':
        _list.entrypoint(args)
    elif s == 'tag':
        _tag.entrypoint(args)
    elif s == 'version':
        _version.entrypoint(args)
    else:
        assert False
