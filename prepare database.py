import json
import energima_logger
import logging
from SauterAPI_module_v3_2 import sautervision_login, sautervision_sensorlist
from CogniteAPI_module_sauter_v1 import cdp_startup
from monotonic import monotonic
import os
import threading


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
    cdp_startup(logger)
    ip = cdp_startup.configuration["energima"]["ip_address"]
    un_env = cdp_startup.configuration["energima"]["login_un"]
    un = os.getenv(un_env)
    pw_env = cdp_startup.configuration["energima"]["login_pw"]
    pw = os.getenv(pw_env)
    sautervision_login(logger, ipaddress=ip, username=un, password=pw)

    return energima_startup.start_time, logger, sautervision_login.jar


def request_sensorlist():
    logger = logging.getLogger("prepare_database")

    sensors = sautervision_sensorlist(logger, ip, sautervision_login.jar)
    print(sensors)
    with open('total_sensorlist.json', 'w') as data_file:
        json.dump(sensors, data_file)


active = []
inactive = []


def sensordata_func(sensor):
    lock.acquire()
    sensor_data = sautervision_dataretrieval_func(ipaddr, sensor["Id"])
    lock.release()

    if sensor_data["HistoricalDataValues"] != []:
        active.append(sensor)
        logger.info("active", active)
        with open('active_sensors.json', 'w') as outfile:
            json.dump(active, outfile, ensure_ascii=False)
    else:
        inactive.append(sensor)
        logger.info("inactive", inactive)
        with open('inactive_sensors.json', 'w') as outfile:
            json.dump(inactive, outfile, ensure_ascii=False)


def threading_func():
    # The worker thread pulls an item from the queue and processes it
    def worker():
        while True:
            sensor = q.get()
            sensordata_func(sensor)
            q.task_done()

    # assign workers, and loop trough queue
    q = Queue()  # Queue for sensors

    try:
        for i in range(1):
            t = threading.Thread(target=worker)
            t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
            t.start()
    except:
        logger.error("Failed to start Threads!")  # loginfo----------------------------------------------
    try:

        for sensor in sautervision_objectlist.data:
            q.put(sensor)
    except:
        logger.error("Failed to loop over sensors in Threading func!")  # loginfo---------------------------------
    print(Queue.qsize(q))

    q.join()


def json_cleaner_active():
    with open('objectlist.json') as data_file:
        list = json.load(data_file)
    print("Total active sensors: ", len(list))

    for item in list:
        if 'device,908' in item:
            del item['device,908']
        if "IsActive" in item:
            del item["IsActive"]
        if "BindingType" in item:
            del item["BindingType"]
        if "ObjectType" in item:
            del item["ObjectType"]
        if "Connection" in item:
            del item["Connection"]
        if "Device" in item:
            del item["Device"]
        if "Unit" in item:
            del item["Unit"]
        if "AlarmConditionsEnabled" in item:
            del item["AlarmConditionsEnabled"]

    with open('clean_objectlist.json', 'w') as data_file:
        json.dump(list, data_file)


def clean_sensorlist():
    with open('active_sensors.json', 'r') as object_list:
        sensors = json.load(object_list)
        object_list.close()
        print(sensors["Name"])

    clean_list = sensors["Name"].replace(".", "_")

    with open('active_sensors.json', 'w') as data_file:
        json.dump(clean_list, data_file)


if __name__ == "__main__":
    lock = threading.Lock()
    # energima_startup()
    clean_sensorlist()
    # request_sensorlist()
