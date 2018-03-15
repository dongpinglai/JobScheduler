#!/usr/bin/env python
# -*-encoding: utf-8-*-

from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore

mongo_host = "localhost"
mongo_port = 27017
# 设置任务存储器
jobstores = {
    "default": MongoDBJobStore(connect_args={"host": mongodb_host, "port": mongodb_port}),
}
# 设置任务执行器
executores = {
    "default": TornadoExecutor()
}

# 设置任务配置为空，取用框架的默认配置
job_defaults = {}
# apsheduler的调度器实例
scheduler = TornadoScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)


class JobScheduler(object):
    def __init__(self):
        self._scheduler = scheduler

    def add_job(self, func, *args, **kwargs):
        """
        新增任务
        """
        self._scheduler.add_job(func, *args, **kwargs)

    def modify_job(self, job_id, jobstore=None, **changes):
        """改变任务属性"""
        self._scheduler.modify_job(job_id, jobstore, **change)

    def reschedule_job(self, job_id, jobstore=None, trigger=None, **trigger_args):
        """重新设置任务启动模式"""
        self._scheduler.reschedule_job(job_id, jobstore, trigger, **trigger_args)

    def remove_job(self, job_id, jobstore=None):
        """删除任务"""
        self._scheduler.remove_job(job_id, jobstore)

