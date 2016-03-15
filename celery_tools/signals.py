# -*- coding: utf-8 -*-
"""
Celery signals.
"""
import logging

from celery import current_app


def prevent_single_task_duplication(sender, body, **kwargs):
    # Get task class
    task = current_app.tasks.get(sender)

    logger = logging.getLogger('celery.tasks')

    # Check if is a Single task and revoke if is being executed
    if getattr(task, 'single_run', False) and hasattr(task, 'lock') and task.lock.locked():
        logger.info("%s > Task (%s) revoked due is currently being executed", task.TAG, body['id'])
        current_app.control.revoke(body['id'])
