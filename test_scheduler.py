#!/usr/bin/env python
# -*- encoding:utf-8 -*-


import requests
import json
import os
from functools import wraps
import settings


def get_trigger_info(push_type):
    """配置触发器"""
    triggers = {
        # 每天
        "every_day": ("cron", {"day": "*/1", "hour": "11"}),
        # 每三天
        "every_three_days": ("cron", {"day": "*/3", "hour": "11"}),
        # 每七天
        "every_seven_days": ("cron", {"day": "*/7", "hour": "10"})
    }
    if push_type in triggers:
        return triggers[push_type]
    return None


def add_file_path(execute):
    script_file_path = os.path.abspath(__file__)
    @wraps(execute)
    def wrapper(context):
        params = context.params 
        params["__script_file_path__"] = script_file_path
        execute(context)
    
    return wrapper


@add_file_path
def execute(context):
    params = context.params
    if params:
        func_name = params.get("func_name")
        arguments = params.get("arguments", {})
        push_type = params.get("push_type")
        caseid = arguments.get("caseid")
        if not caseid:
            context.result = {"status": False, "msg": "caseid not in params or caseid is None"}
            return

        if not func_name:
            context.result = {"status": False, "msg": "func_name not in params or func_name is None"}
            return
        if push_type not in ["every_day", "every_three_days", "every_seven_days"]:
            context.result = {"status": False, "msg": "push_type value is wrong"}
            return
        params["job_id"] = str(caseid)
        trigger_type, trigger_args = get_trigger_info(push_type)
        params["trigger_type"] = trigger_type
        params["trigger_args"] = trigger_args
        url = "http://127.0.0.1:56789/job_schedule" 
        try:
            res = requests.post(url, data={"params": json.dumps(params)})
        except:
            context.result = {"status": False, "msg": "HTTP Error, url:%s, params:%s" % (url, params)} 
            return 
        else:
            result = res.json()
            print result
            context.result = result
    else:
        context.result = {"status": True, "msg": "params is None"}
    
    
def my_print(*args, **kwargs):
    import datetime
    print datetime.datetime.now(), args, kwargs

if __name__ == "__main__":
    from collections import namedtuple

    context = namedtuple("context", "params")
    context.params = {
        #"func_name": "export_topic",
        "func_name": "my_print",
        "arguments": {
            "email_to": "",
            "caseid": "1",
        },
        #"push_type": "every_three_days",
        "push_type": "every_day",
        "is_remove": False
        #"is_remove": True
    }
    execute(context)

