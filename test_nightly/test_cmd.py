#!/usr/bin/env python3
# coding: utf-8

from __future__ import division, unicode_literals
from joker.nightly.protocol import Command, TopCommand


class CmdA(Command):
    name = 'apple'
    desc = 'eat some apple'

    @classmethod
    def add_arguments(cls, parser=None):
        parser.add_argument(
            '-a', '--alpha', action='store_true',
            help='the alpha option',
        )

    @classmethod
    def execute(cls, **params):
        print(cls.name, params)


class CmdB(Command):
    name = 'banana'
    desc = 'eat some banana'

    @classmethod
    def add_arguments(cls, parser=None):
        parser.add_argument(
            '-b', '--beta', action='store_true',
            help='the beta option',
        )

    @classmethod
    def execute(cls, **params):
        print(cls.name, params)


class CmdTop(TopCommand):
    @classmethod
    def add_arguments(cls, parser=None):
        parser.add_argument(
            '-y', '--yes', action='store_true',
            help='proceed without confirmation'
        )


def run():
    CmdTop.register(CmdA)
    CmdTop.register(CmdB)
    CmdTop.run()


if __name__ == '__main__':
    run()
