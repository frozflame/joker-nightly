#!/usr/bin/env python3
# coding: utf-8

from __future__ import division, print_function

import concurrent.futures
import datetime
import logging
import multiprocessing
import os
import sys
import time
import traceback

from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler


class ExecutorRejective(Exception):
    pass


class RobustProcessPoolExecutor(ProcessPoolExecutor):
    """
    Handle this exception:
        concurrent.futures.process.BrokenProcessPool
    """
    def __init__(self):
        self.rejective = False
        self.max_workers = max(4, multiprocessing.cpu_count())
        super(RobustProcessPoolExecutor, self).__init__(self.max_workers)

    def submit_job(self, job, run_times):
        """
        In case of a subprocess is killed and a BrokenProcessPool exception
        is raised, re-create the process pool.
        :param job:
        :param run_times:
        :return:
        """
        if self.rejective:
            msg = 'executor reviving'
            raise ExecutorRejective(msg)
        try:
            super(RobustProcessPoolExecutor, self).submit_job(job, run_times)
        except concurrent.futures.process.BrokenProcessPool:
            self.rejective = True
            traceback.print_exc()
            msg = 'RE-CREATING PROCESS POOL ..'
            logging.info(msg)
            self._pool.shutdown(wait=True)
            self._pool = concurrent.futures.ProcessPoolExecutor(self.max_workers)
            self.rejective = False
        except:
            raise


class NightlySchedule(object):
    def __init__(self, *a, **kw):
        self.options = kw.pop('nightly_options', dict())
        executor = RobustProcessPoolExecutor()
        self.scheduler = BackgroundScheduler(*a, **kw)
        self.scheduler.add_executor(executor)
        self.avail_tasks = dict()
        self.pid = os.getpid()

    def get_path(self, filename):
        if filename.startswith('/'):
            return filename
        d = self.options.get('dir', '/data1/frozflame/nightly')
        return os.path.join(d, filename)

    def open_file(self, filename, mode):
        if filename is None:
            return None
        if filename == 'stdout':
            return sys.stdout
        elif filename == 'stderr':
            return sys.stderr
        else:
            return open(self.get_path(filename), mode)

    def check_sunrise(self):
        # from statscv.framework.logging import LoggerVisualizer
        # LoggerVisualizer.debug()
        path = self.get_path('sunrise')
        return os.path.isfile(path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.remove_pid()

    def read(self, filename):
        if not filename:
            return []
        infile = self.open_file(filename, 'r')
        with infile:
            try:
                lines = [l.strip() for l in infile]
            except IOError:
                lines = []
        return [l for l in lines if not l.startswith('#')]

    def read_blacklist(self):
        filename = self.options.get('filename_blacklist')
        return frozenset(self.read(filename))

    def write(self, filename, items, commented=True):
        if not filename:
            return
        outfile = self.open_file(filename, 'w')
        tpl = '# {}' if commented else '{}'
        lines = [tpl.format(it) for it in items]
        lines.append('# {:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()))
        content = '\n'.join(lines) + '\n'
        with outfile:
            outfile.write(content)

    def write_added_jobs(self):
        filename = self.options.get('filename_added_jobs')
        job_ids = {j.id for j in self.scheduler.get_jobs()}
        self.write(filename, job_ids, commented=True)

    def write_avail_jobs(self):
        filename = self.options.get('filename_avail_jobs')
        job_ids = list(self.avail_tasks)
        self.write(filename, job_ids, commented=True)

    def write_pid(self):
        filename = self.options.get('filename_pid')
        self.write(filename, [self.pid], commented=False)

    def remove_pid(self):
        filename = self.options.get('filename_pid')
        if filename:
            os.remove(self.get_path(filename))

    def reschedule_nightly(self):
        """
        add / remove jobs according to blacklist.txt
        """
        job_ids = frozenset(self.avail_tasks)
        job_ids_added = {j.id for j in self.scheduler.get_jobs()}
        job_ids_black = self.read_blacklist()

        job_ids_to_add = job_ids - job_ids_added - job_ids_black
        job_ids_to_remove = job_ids_added.intersection(job_ids_black)

        for jid in job_ids_to_remove:
            self.scheduler.remove_job(jid)

        for jid in job_ids_to_add:
            task = self.avail_tasks[jid]
            self.scheduler.add_job(**task.params)

    def add_nightly_task(self, task):
        """
        :param task: a NightlyTask instance
        """
        self.avail_tasks[task.job_id] = task
        self.scheduler.add_job(**task.params)

    def start(self, *a, **kw):
        self.scheduler.start(*a, **kw)
        date = datetime.date.today()
        while date == datetime.date.today():
            if self.check_sunrise():
                break
            self.reschedule_nightly()
            self.write_added_jobs()
            self.write_avail_jobs()
            self.write_pid()
            time.sleep(5)

    def run_nightly_tasks(self, tasks):
        """
        :param tasks: a list of dicts or NightlyTask instances
        :return:
        """
        if self.check_sunrise():
            return
        for task in tasks:
            self.add_nightly_task(task)
        self.start()
