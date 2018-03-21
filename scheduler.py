#!/usr/bin/env python
# -*-encoding: utf-8-*-

from tornado.web import RequestHandler, Application
from tornado import ioloop
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.util import undefined
import json

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
    _scheduler = scheduler
    # 记录可以用任务函数
    _job_funcs= {} 
    def __init__(self):
        self.initialize()
    
    def self.initialize(self):
        """可以将要用的任务函数在这里注册"""
        pass

    def register_func(self, func_name, func):
        self._job_funcs[func_name] = func
        
    def remove_func(self, func_name):
        self._job_funcs.pop(func_name, None)

    def update_func(self, func_name, func):
        self._job_funcs[func_name] = func

    def find_func(self, func_name):
        func = self._job_funcs.get(func_name)
        return func

    def upsert_job(self, func_name, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore="default", executor="default", replace_existing=False, **trigger_args):
        """
        新增任务或更新任务
        1.每个任务都预先设置一个id
        2.如果id已经有一个任务，那就执行更新任务操作,否则，新增一个任务
        """
        if not callable(func_name):
            func = self.find_func(func_name)
        if func and callable(func):
            job_id = id
            job = self.get_job(job_id)
            if job is None:
                self.add_job(func, trigger, args, kwargs, id, name, misfire_grace_time, coalesce, max_instances, next_run_time, jobstore, executor, replace_existing, **trigger_args)
            else:
                changes = {}
                changes = {"func": func, "args": args, "kwargs": kwargs, "name": name, "misfire_grace_time": misfire_grace_time, "coalesce": coalesce, "max_instances": max_instances, "trigger": trigger, "executor": executor, "next_run_time": next_run_time}
                self.modify_job(job_id, jobstore, **changes)
        else:
            raise TypeError("%s不是可调用对象" % func)

    def add_job(self, func, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, coalesce=undefined, max_instances=undefined, next_run_time=undefined, jobstore="default", executor="default", replace_existing=False, **trigger_args):
        """新增任务"""
        self._scheduler.add_job(func, trigger, args, kwargs, id, name, misfire_grace_time, coalesce, max_instances, next_run_time, jobstore, executor, replace_existing, **trigger_args)

    def modify_job(self, job_id, jobstore=None, **changes):
        """更新改变任务属性"""
        self._scheduler.modify_job(job_id, jobstore, **change)

    def reschedule_job(self, job_id, jobstore=None, trigger=None, **trigger_args):
        """重新设置任务启动模式"""
        self._scheduler.reschedule_job(job_id, jobstore, trigger, **trigger_args)

    def remove_job(self, job_id, jobstore=None):
        """删除任务"""
        self._scheduler.remove_job(job_id, jobstore)

    def get_job(self, job_id, jobstore=None):
        """获取任务"""
        return self._scheduler.get_job(job_id, jobstore)


class JobHanlder(RequestHandler):
    # 全局变量
    job_scheduler = JobScheduler()
    def post(self):
        func_name = self.get_argument("func_name")
        push_type = self.get_argument("push_type")
        arguments = self.get_argument("arguments")
        arguments = json.loads(arguments) if isinstance(arguments, basestring) else arguments
        trigger_and_args = self.get_trigger_and_args_from_push_type(push_type)
        if trigger_and_args is None:
            result = {"status": False, "msg": "没有可用的触发器"}
            self.write(json.dumps(result))
            return
        if not isinstance(arguments, dict):
            result = {"status": False, "msg": "参数错误"}
            self.write(json.dumps(result))
            return 
        job_id = arguemnts["case_id"]
        trigger, trigger_args = trigger_and_args
        try:
            self.job_scheduler.upsert_job(func_name, trigger, kwargs=arguments, **trigger_args)
        except:
            result = {"status": False, "msg": "新增任务/更新任务失败"}
            self.write(json.dumps(result))
            self.log()
        else:
            result = {"status": True, "msg": "新增任务/更新任务成功"}
            self.write(json.dumps(result))
            

    def get_trigger_and_args_from_push_type(self, push_type):
        """配置触发器"""
        triggers = {
            # 每天
            "every_day": ("cron", {"day": "*/1"}),
            # 每三天
            "every_three_days": ("cron", {"day": "*/3"}),
            # 每七天
            "every_seven_days": ("cron", {"day": "*/7"})
        }
        if push_type in triggers:
            return triggers[push_type]
        return None
        
    def log(self, msg):
        """记录日志"""
        pass

if __name__ == "__main__":
    
    pass

