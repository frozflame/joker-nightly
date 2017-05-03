#!/usr/bin/env python3
# coding: utf-8

from __future__ import division, print_function

import argparse
import copy
import sys
import traceback

from joker.broker.logging import LoggerBroker
from joker.nightly import compat


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
    LoggerBroker.primary_logger_name = options.get('id')

    with compat.redirect_stdout(o), compat.redirect_stderr(e):
        try:
            func(*a, **kw)
        except:
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
        LoggerBroker.config_logger(name, log_level, path)

    @staticmethod
    def conf_aps_logger(level='INFO'):
        """config apscheduler logger"""
        LoggerBroker.config_logger(None, level)
        LoggerBroker.config_logger('apscheduler.scheduler', level)


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
    parser_params = {}

    @classmethod
    def parse_args(cls, raw_args=None):
        """
        必须保证该方法在 raw_args == [] 时不出错
        :param raw_args:
        :return:
        """
        raise NotImplementedError

    @classmethod
    def example_parse_args(cls, raw_args=None):
        parser = argparse.ArgumentParser(description='a statscv command')
        parser.add_argument('-c', '--config', default='offline')
        return parser.parse_args(args=raw_args)

    @classmethod
    def execute(cls, **params):
        """
        该方法中，使用 params['option'] 来获取参数，而不是 params.get('option')
        缺少参数直接抛出异常; 由 run 方法保证所有参数传入
        """
        raise NotImplementedError

    @classmethod
    def run(cls, main=False, **params):
        """
        :param main: 仅当需要从 sys.argv 获取参数时，传入 True
        :param params:
        :return:
        """
        if main:
            nsp = cls.parse_args()
        else:
            # 获取参数默认值
            # parse_args 方法须保证 raw_args == [] 时不出错
            nsp = cls.parse_args([])
        p = vars(nsp)
        p.update(params)
        cls.execute(**p)


class TopCommand(object):
    # TODO: remove this
    name = 'statscv'
    subs = dict()

    @classmethod
    def parse_args(cls, raw_args=None):
        params = {
            'usage': '{} [-h] subcommand ...'.format(sys.argv[0]),
            'add_help': False
        }
        parser = argparse.ArgumentParser(**params)
        parser.add_argument('subcommand', help='options: {}'.format(','.join(cls.subs.keys())))
        cls.parser = parser
        return parser.parse_known_args(args=raw_args)

    @classmethod
    def register(cls, subcmd):
        name = getattr(subcmd, 'name') or subcmd.__name__
        cls.subs[name] = subcmd

    @classmethod
    def run(cls):
        nsp, extra_args = cls.parse_args()
        subcmd = cls.subs.get(nsp.subcommand)
        if not subcmd:
            cls.parser.print_help()
            return

        print('ok', subcmd, extra_args)
        if subcmd:
            subcmd.parser_params['prog'] = '{} {}'.format(sys.argv[0], nsp.subcommand)
            nsp = subcmd.parse_args(raw_args=extra_args)
            subcmd.execute(**vars(nsp))
