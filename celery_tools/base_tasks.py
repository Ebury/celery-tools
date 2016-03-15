# -*- coding: utf-8 -*-
"""
Base tasks for celery.
"""
from celery import current_app
from celery.utils.log import get_task_logger

from celery_tools.concurrency import CacheLock

LOGGER_DEFAULT_NAME = __name__


class LoggedTask(current_app.Task):
    TAG = ''
    abstract = True

    def __init__(self, logger=None):
        super(LoggedTask, self).__init__()
        if logger is None:
            logger = get_task_logger(LOGGER_DEFAULT_NAME)

        self.logger = logger

    def __call__(self, *args, **kwargs):
        self.logger.info("%s > Task started", self.TAG)
        return super(LoggedTask, self).__call__(*args, **kwargs)

    def run(self, *args, **kwargs):
        super(LoggedTask, self).run(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        self.logger.info("%s > Task (%s) completed with result: %s", self.TAG, str(task_id), str(retval))
        super(LoggedTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.logger.error("%s > Task (%s) failed", self.TAG, str(task_id), exc_info=exc)
        super(LoggedTask, self).on_failure(exc, task_id, args, kwargs, einfo)


class LoggedSingleTask(LoggedTask):
    abstract = True
    single_run = True

    def __init__(self, *args, **kwargs):
        super(LoggedSingleTask, self).__init__(*args, **kwargs)
        lock_id = 'lock_{}'.format(self.TAG.lower())
        self.lock = CacheLock(cache_key=lock_id)

    def __call__(self, *args, **kwargs):
        if self.lock.acquire():
            result = super(LoggedSingleTask, self).__call__(*args, **kwargs)
            self.lock.release()
            return result
        else:
            return False

    def on_success(self, retval, task_id, args, kwargs):
        self.lock.release()
        super(LoggedSingleTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.lock.release()
        super(LoggedSingleTask, self).on_failure(exc, task_id, args, kwargs, einfo)



