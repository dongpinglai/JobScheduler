#!/usr/bin/env python
# -*-encoding: utf-8-*-

from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.util import undefined

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
        # 记录可以用任务函数
        self._job_funcs= {}

    def register_func(self, func_name, func):
        self._job_funcs[func_name] = func
        
    def remove_func(self, func_name):
        self._job_funcs.pop(func_name, None)

    def update_func(self, func_name, func):
        self._job_funcs[func_name] = func

    def find_func(self, func_name):
        func = self._job_funcs.get(func_name)
        return func

    def add_job(self, func_name, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore="default", executor="default", replace_existing=False, **trigger_args):
        """
        新增任务
        1.每个任务都预先设置一个id
        2.如果id已经有一个任务，那就执行更新任务操作,否则，新增一个任务
        """
        func = self.find_func(func_name)
        if func and callable(func):
            self._scheduler.add_job(func, trigger, args, kwargs, id, name, misfire_grace_time, coalesce, max_instances, next_run_time, jobstore, executor, replace_existing, **trigger_args)
        else:
            raise TypeError("%s不是可调用对象" % func)

    def modify_job(self, job_id, jobstore=None, **changes):
        """改变任务属性"""
        self._scheduler.modify_job(job_id, jobstore, **change)

    def reschedule_job(self, job_id, jobstore=None, trigger=None, **trigger_args):
        """重新设置任务启动模式"""
        self._scheduler.reschedule_job(job_id, jobstore, trigger, **trigger_args)

    def remove_job(self, job_id, jobstore=None):
        """删除任务"""
        self._scheduler.remove_job(job_id, jobstore)

