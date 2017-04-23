#!/usr/bin/env python3
# coding: utf-8

from __future__ import division, print_function

import sys
import contextlib


class _RedirectStream:
    _stream = None

    def __init__(self, new_target):
        self._new_target = new_target
        # We use a list of old targets to make this CM re-entrant
        self._old_targets = []

    def __enter__(self):
        self._old_targets.append(getattr(sys, self._stream))
        setattr(sys, self._stream, self._new_target)
        return self._new_target

    def __exit__(self, exctype, excinst, exctb):
        setattr(sys, self._stream, self._old_targets.pop())


class _RedirectSTDOUT(_RedirectStream):
    _stream = "stdout"


class _RedirectSTDERR(_RedirectStream):
    _stream = "stderr"


if (sys.version_info.major, sys.version_info.minor) < (3, 5):
    redirect_stdout = _RedirectSTDOUT
    redirect_stderr = _RedirectSTDERR
else:
    redirect_stdout = contextlib.redirect_stdout
    redirect_stderr = contextlib.redirect_stderr
