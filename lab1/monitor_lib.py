# monitor_lib.py
# -*- coding:utf-8 -*-

import json
import psutil

def get_cpu_status():
    cpu_usage = psutil.getloadavg()  # 1, 5, 15 min averages
    return {'lavg_1': cpu_usage[0], 'lavg_5': cpu_usage[1], 'lavg_15': cpu_usage[2]}

def get_memory_status():
    memory_info = psutil.virtual_memory()
    return {
        'total': memory_info.total,
        'available': memory_info.available,
        'used': memory_info.used,
        'free': memory_info.free,
        'percent': memory_info.percent
    }

def collect_resource_status():
    status = {
        "cpu": get_cpu_status(),
        "memory": get_memory_status()
    }
    return json.dumps(status)
