#!/usr/bin/env python
# -*-encoding: utf-8-*-

"""
DOC
"""

from tornado.web import RequestHandler, Application
from tornado import ioloop
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import undefined
import json
import uuid

mongodb_host = "localhost"
mongodb_port = 27017
# 设置任务存储器
connect_args = {"host": mongodb_host, "port": mongodb_port}
jobstores = {
    "default": MongoDBJobStore(**connect_args),
}
# 设置任务执行器
executors = {
    "default": TornadoExecutor()
}

# 设置任务配置为空，取用框架的默认配置
job_defaults = {}
# apsheduler的调度器实例
scheduler = TornadoScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults)

class JobScheduler(object):
    _scheduler = scheduler
    # 记录可以用任务函数
    _job_funcs = {}

    def __init__(self):
        self.initialize()

    def initialize(self):
        """可以将要用的任务函数在这里注册"""
        self._job_funcs["my_print"] = my_print

    def register_func(self, func_name, func):
        self._job_funcs[func_name] = func

    def remove_func(self, func_name):
        self._job_funcs.pop(func_name, None)

    def update_func(self, func_name, func):
        self._job_funcs[func_name] = func

    def find_func(self, func_name):
        func = self._job_funcs.get(func_name)
        return func

    def upsert_job(self, func_name, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, coalesce=undefined,
                   max_instances=undefined, next_run_time=undefined, jobstore="default", executor="default", replace_existing=False, **trigger_args):
        """
        新增任务或更新任务
        1.每个任务都预先设置一个job_id
        2.如果job_id已经有一个任务，那就执行更新任务操作,否则，新增一个任务
        """
        if not callable(func_name):
            func = self.find_func(func_name)
        if func and callable(func):
            job_id = id
            job = self.get_job(job_id)
            if job is None:
                self.add_job(
                    func,
                    trigger,
                    args,
                    kwargs,
                    id,
                    name,
                    misfire_grace_time,
                    coalesce,
                    max_instances,
                    next_run_time,
                    jobstore,
                    executor,
                    replace_existing,
                    **trigger_args)
            else:
                changes = {
                    "func": func,
                    "kwargs": kwargs,
                    "coalesce": coalesce,
                    "executor": executor}
                if args:
                    changes["args"] = args
                if name:
                    changes["name"] = name
                if misfire_grace_time and misfire_grace_time > 0:
                    changes["misfire_grace_time"] = misfire_grace_time
                if max_instances and max_instances > 0:
                    changes["max_instances"] = max_instances
                if isinstance(
                        trigger, (CronTrigger, DateTrigger, IntervalTrigger)):
                    changes["trigger"] = trigger
                if next_run_time is not undefined:
                    changes["next_run_time"] = next_run_time
                self.modify_job(job_id, jobstore, **changes)
                # if not isinstance(trigger, (CronTrigger, DateTrigger,
                # IntervalTrigger)):
                self.reschedule_job(job_id, jobstore, trigger, **trigger_args)
        else:
            raise TypeError("%s不是可调用对象" % func)

    def add_job(self, func, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, coalesce=undefined,
                max_instances=undefined, next_run_time=undefined, jobstore="default", executor="default", replace_existing=False, **trigger_args):
        """新增任务"""
        print "add_job", func, trigger, id
        self._scheduler.add_job(
            func,
            trigger,
            args,
            kwargs,
            id,
            name,
            misfire_grace_time,
            coalesce,
            max_instances,
            next_run_time,
            jobstore,
            executor,
            replace_existing,
            **trigger_args)

    def modify_job(self, job_id, jobstore=None, **changes):
        """更新改变任务属性"""
        print "modify_job", job_id
        self._scheduler.modify_job(job_id, jobstore, **changes)

    def reschedule_job(self, job_id, jobstore=None,
                       trigger=None, **trigger_args):
        """重新设置任务启动模式"""
        self._scheduler.reschedule_job(
            job_id, jobstore, trigger, **trigger_args)

    def remove_job(self, job_id, jobstore=None):
        """删除任务"""
        print "remove_job", job_id
        self._scheduler.remove_job(job_id, jobstore)

    def get_job(self, job_id, jobstore=None):
        """获取任务"""
        return self._scheduler.get_job(job_id, jobstore)


class JobHandler(RequestHandler):
    # 全局变量
    job_scheduler = JobScheduler()

    def post(self):
        params = self.get_body_argument("params")
        params = json.loads(params) if isinstance(
            params, basestring) else params
        # 预先给定一个任务id
        job_id = params.get("job_id") or str(uuid.uuid1())
        func_name = params.get("func_name")  # 定时调用的函数名
        trigger_type = params.get("trigger_type")  # 触发器类型
        trigger_args = params.get("trigger_args")  # 触发器参数
        arguments = params.get("arguments")  # 调用函数传参
        is_remove = params.get("is_remove")  # 是否删除定时调用任务
        trigger_and_args = self.get_trigger(trigger_type, trigger_args)
        if trigger_and_args is None:
            result = {"status": False, "msg": "没有可用的触发器"}
            self.write(json.dumps(result))
            return
        if not isinstance(arguments, dict):
            result = {"status": False, "msg": "参数错误"}
            self.write(json.dumps(result))
            return
        if is_remove:
            if self.job_scheduler.get_job(job_id):
                try:
                    self.job_scheduler.remove_job(job_id)
                except Exception as e:
                    result = {
                        "status": False, "msg": u"取消任务失败:%s" %
                        e, "job_id": job_id}
                    self.write(json.dumps(result))
                    return
                else:
                    result = {
                        "status": True,
                        "msg": u"取消任务成功",
                        "job_id": job_id}
                    self.write(json.dumps(result))
                    return
            else:
                result = {"status": True, "msg": u"取消任务成功", "job_id": job_id}
                self.write(json.dumps(result))

        else:
            trigger, trigger_args = trigger_and_args
            try:
                self.job_scheduler.upsert_job(
                    func_name, trigger, kwargs=arguments, id=job_id, **trigger_args)
            except Exception as e:
                result = {
                    "status": False,
                    "msg": u"新增任务/更新任务失败:%s" %
                    e,
                    "job_id": job_id}
                self.write(json.dumps(result))
            else:
                result = {
                    "status": True,
                    "msg": "新增任务/更新任务成功",
                    "job_id": job_id}
                self.write(json.dumps(result))

    def get_trigger(self, trigger_type, trigger_args):
        """获取触发器,共有三种类型的触发器:
        1.cron
        2.interval
        3.date
        """
        trigger_classes = {
            "cron": CronTrigger,
            "interval": IntervalTrigger,
            "date": DateTrigger
        }
        if trigger_type in trigger_classes:
            trigger = trigger_classes[trigger_type](**trigger_args)
            return trigger, trigger_args
        else:
            None

    def log(self, msg):
        """TODO:记录日志"""
        pass


def main():
    try:
        scheduler.start()
    except BaseException:
        pass
    app = Application([
        (r"/job_schedule", JobHandler)
    ], settings={"debug": True})
    app.listen(56789)
    ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
