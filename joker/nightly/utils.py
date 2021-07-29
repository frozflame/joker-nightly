#!/usr/bin/env python3
# coding: utf-8


_Task = namedtuple('Task', ['func', 'args', 'kwargs'])


def standardize_task(task: Union[list, dict, _Task]) -> _Task:
    if isinstance(task, _Task):
        return task
    if not isinstance(task, (list, dict)):
        t = type(task)
        raise TypeError(f'task must be a dict or list, got {t}')
    if not task:
        raise ValueError(f'invalid task: {task}')
    if isinstance(task, list):
        return _Task(task[0], task[1:], {})
    if isinstance(task, dict):
        func = task.pop('func')
        args = task.pop('args', tuple())
        kwargs = task.pop('kwargs', {})
        kwargs.update(task)
        return _Task(func, args, kwargs)
