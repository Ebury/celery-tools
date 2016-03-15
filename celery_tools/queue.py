# -*- coding: utf-8 -*-
"""
Queue utils.
"""
from celery.bin.amqp import amqp


def clear_queue(app: 'celery.Celery', name: str):
    """
    Clear a Celery queue.

    :param app: Celery app.
    :param name: Queue name.
    """
    manager = amqp(app=app)
    manager.run('queue.purge', name)
