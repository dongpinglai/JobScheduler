#!/usr/bin/env python
# -*-encoding: utf-8-*-

import os


SCHEDULER_USE_PORT = True
SCHEDULER_TORNADO_PORT = 56789
SCHEDULER_MONGODB_HOST = "localhost"
SCHEDULER_MONGODB_PORT = 27017
SCHEDULER_UNIX_SOCK_PATH = os.path.dirname(__file__) + "scheduler.sock"

