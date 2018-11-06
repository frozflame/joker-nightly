#!/usr/bin/env python3
# coding: utf-8

from __future__ import division, print_function

import argparse
import copy
import logging
import sys
import traceback

from joker.nightly import compat


class LoggerScope(object):
    """
    mostly just do this in each module:
        loggerscope = LoggerScope(__name__)

    and use like:
        loggerscope.logger.info('data loaded')
    """

    primary_logger_name = ''

    def __init__(self, name):
        self.name = name

    @classmethod
    def config_logger(cls, name, log_level, path=None):
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logger.handlers = []
        if path:
            handler = logging.FileHandler(path)
        else:
            handler = logging.StreamHandler()
        handler.setLevel(log_level)

        fmt = '%(asctime)s [%(process)d] [%(levelname)s] %(message)s'
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)

    @property
    def logger(self):
        return logging.getLogger(self.primary_logger_name or self.name)


def standard_func(func, options, *a, **kw):
    """
    :param func:
    :param options: a dict
    :param a:
    :param kw:
    :return:
    """
    # this one should be done inside `func`
    # ResourceBroker.just_after_fork()
    stdout = options.get('stdout')
    stderr = options.get('stderr')
    o = open(stdout, 'a') if stdout else sys.stdout
    e = open(stderr, 'a') if stderr else sys.stderr
    LoggerScope.primary_logger_name = options.get('id')

    with compat.redirect_stdout(o), compat.redirect_stderr(e):
        try:
            func(*a, **kw)
        except Exception:
            traceback.print_exc()


class NightlyTask(object):
    def __init__(self, params):
        """
        convert a dict into valid kwargs for scheduler.add_job

        add_job(self,
          func, trigger=None, args=None, kwargs=None, id=None, name=None,
          misfire_grace_time=undefined, coalesce=undefined, max_instances=undefined,
          next_run_time=undefined, jobstore='default', executor='default',
          replace_existing=False, **trigger_args): ...

        :param params: a dict
        :return:
        """
        if not params.get('id'):
            raise ValueError('missing "id" field or incorrect value')
        params = copy.deepcopy(params)
        nightly_options = params.pop('nightly_options', {})
        nightly_options['id'] = params['id']
        func = params['func']
        args = params.get('args') or []
        params['func'] = standard_func
        params['args'] = [func, nightly_options] + args
        params['name'] = params.get('name') or params.get('id')
        params['max_instances'] = params.get('max_instances') or 1
        self.job_id = params['id']
        self.params = params
        self.nightly_options = nightly_options

    @classmethod
    def batch_create(cls, records):
        skeds = []
        for record in records:
            if 'id' not in record:
                continue
            sked = cls(record)
            skeds.append(sked)
        for sked in skeds:
            sked.conf_logger()
        return skeds

    def conf_logger(self):
        name = self.nightly_options.get('id')
        path = self.nightly_options.get('stderr')
        log_level = self.nightly_options.get('log_level', 'INFO')
        LoggerScope.config_logger(name, log_level, path)

    @staticmethod
    def conf_aps_logger(level='INFO'):
        """config apscheduler logger"""
        LoggerScope.config_logger(None, level)
        LoggerScope.config_logger('apscheduler.scheduler', level)


class ExclusiveJobRunning(Exception):
    pass


class ExclusiveJob(object):
    RUNNING_INDICATOR = None
    RUNNING_INDICATOR_EXPIRE_AFTER = 120

    def __init__(self, kvstore):
        """
        :param kvstore: e.g. a redis.StrictRedis instance
        """
        self.kvstore = kvstore

    @classmethod
    def get_running_indicator(cls):
        if cls.RUNNING_INDICATOR:
            return cls.RUNNING_INDICATOR
        cn = cls.__name__
        return 'piilabs:ExclusiveJob:{}'.format(cn)

    def preempt(self):
        key = self.get_running_indicator()
        # TODO: fix the atomicity problem
        if self.kvstore.get(key):
            cn = self.__class__.__name__
            msg = 'an instance of {} is already running'.format(cn)
            raise ExclusiveJobRunning(msg)
        self.kvstore.setex(key, self.RUNNING_INDICATOR_EXPIRE_AFTER, 1)

    def resign(self):
        key = self.get_running_indicator()
        self.kvstore.delete(key)

    def __enter__(self):
        self.preempt()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.resign()


class ResumableJob(object):
    def __init__(self, kvstore, key, start):
        self.key = key
        self.kvstore = kvstore
        self.position = self.get_position(start)

    def step(self):
        return

    def proceed(self):
        while self.step():
            self.flush_position()

    def get_position(self, start):
        return self.kvstore.get(self.key, default=start)

    def flush_position(self):
        self.kvstore.set(self.key, self.position)


class Command(object):
    name = ''
    desc = ''

    @classmethod
    def add_arguments(cls, parser=None):
        """
        Make sure every argument has a default or is optional
        """
        raise NotImplementedError

    @classmethod
    def parse_arguments(cls, raw_args=None):
        """
        :param raw_args:
        :return: an `argparse.Namespace` instance
        """
        parser = argparse.ArgumentParser(description=cls.desc)
        cls.add_arguments(parser)
        return parser, parser.parse_args(raw_args)

    @classmethod
    def execute(cls, **params):
        """
        In this method, plz access parameter with params[option],
        instead of params.get(option).
        Exception should be raised if some parameter is missing.
        The `run` method should make sure all requiring parameters are passed
        """
        raise NotImplementedError

    @classmethod
    def run(cls, main=False, **params):
        """
        :param main: bool. If true, access parameters from sys.argv
        :param params: dict. Update argparse result with this dict.
        """
        if main:
            nsp = cls.parse_arguments()
        else:
            # get default arguments
            nsp = cls.parse_arguments([])
        p = vars(nsp)
        p.update(params)
        cls.execute(**p)


class TopCommand(object):
    desc = ''
    subcommands = dict()

    @classmethod
    def add_arguments(cls, parser=None):
        raise NotImplementedError

    @classmethod
    def parse_arguments(cls, raw_args=None):
        parser = argparse.ArgumentParser(description=cls.desc, add_help=False)
        cls.add_arguments(parser)
        subparsers = parser.add_subparsers(dest='subcmd', metavar='subcommand')
        for name, cmd in cls.subcommands.items():
            subp = subparsers.add_parser(name, help=cmd.desc)
            cmd.add_arguments(subp)
        return parser, parser.parse_args(args=raw_args)

    @classmethod
    def register(cls, subcommand):
        cls.subcommands[subcommand.name] = subcommand

    @classmethod
    def run(cls, **params):
        parser, nsp = cls.parse_arguments()
        subcmd = cls.subcommands.get(nsp.subcmd)

        if not subcmd:
            parser.print_help()
            return
            # m = "unknown sub command '{}'".format(subcmd)
            # raise ValueError(m)

        p = vars(nsp)
        p.update(params)
        subcmd.execute(**p)
