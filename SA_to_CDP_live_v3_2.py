import sys
import os
import json
import threading
from queue import Queue
import time
import datetime
import logging
import requests
from monotonic import monotonic
from datetime import timedelta
import energima_logger

# Energima imports
from SauterAPI_module_v3_2 import sautervision_login, local_sensorlist, sautervision_dataprocessing_live

# Cognite mod imports
from CogniteAPI_module_sauter_v1 import upload_objects, upload_datapoints_live, cdp_startup, upload_datapoints_live


def energima_startup():
    """
    setup for energima logger, starts runtime, fetching ip, password and username from config file
    loggs in to API for cookie jar storage and retrives sensorlist
    :return: sensorlist,startime(for runtime), lock for threads and Energima logger
    """
    global un
    global pw
    global ip
    global logger
    logger = logging.getLogger("main")
    energima_logger.configure_logger(
        logger_name="main",
        logger_file="Error_log",
        log_level="INFO", )

    energima_startup.start_time = monotonic()
    energima_startup.lock = threading.Lock()
    cdp_startup(logger)
    ip = cdp_startup.configuration["energima"]["ip_address"]
    un_env = cdp_startup.configuration["energima"]["login_un"]
    un = os.getenv(un_env)
    pw_env = cdp_startup.configuration["energima"]["login_pw"]
    pw = os.getenv(pw_env)
    sautervision_login(logger, ipaddress=ip, username=un, password=pw)
    energima_startup.sensors = local_sensorlist(logger)
    logger.debug(energima_startup.sensors)

    return energima_startup.sensors, energima_startup.start_time, energima_startup.lock, logger


def download_datapoints(logger, sensor):
    """
    Sends sensor id to request func to retrieve data
    adds localtimestamp and parse sensordata to CDP upload
    :param logger: Energima logger
    :param sensor: sensor id
    :return None
    """
    lock = energima_startup.lock

    lock.acquire()
    sensor_data = sautervision_dataprocessing_live(logger, sensor["Id"], ipaddress=ip)
    logger.debug("downloading " + str(sensor["Name"]) + " from API")
    lock.release()
    sensor['PresentValue'] = sensor_data
    now = round(time.time(), 1)
    sensor['LocalTimestamp'] = now
    logger.debug("add PresentValue and LocalTimestamp for " + str(sensor["Name"]))
    upload_datapoints_live(logger, sensor, cdp_startup.api_key, cdp_startup.project_name, cdp_startup.log)


def multithreading(logger):
    """
    worker pulls one sensor from queue and processes it
    :param logger: Energima logger
    :output: sensor from queue for processing in sensordata_func
    """
    def worker():
        while True:
            sensor = q.get()
            download_datapoints(logger, sensor)
            logger.debug("threading_func, sensor id: " + str(sensor))
            q.task_done()

    q = Queue()
    for i in range(20):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
    try:
        for sensor in energima_startup.sensors:
            q.put(sensor)
    except NameError as err:
        logger.error("threading_func: " + str(err))
    except Exception as err:
        logger.error("threading_func: " + str(err))
    else:
        q.join()


if __name__ == "__main__":
    energima_startup()
    multithreading(logger)
    end_time = monotonic()
    print("Session time", timedelta(seconds=end_time - energima_startup.start_time))
