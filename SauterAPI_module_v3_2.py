import json
from datetime import datetime
from datetime import timedelta
import decimal
import requests
import pandas
import logging
import threading
from queue import Queue


def sautervision_login(logger, ipaddress=str, username=str, password=str):
    """
    Function for login to sauter vision center by username, pw and ip-address
    :param logger: Energima logger
    :param ipaddress: ip address for API, loads from config file.
    :param username: username for API, loads from config file (env variable)
    :param password: password for API, loads from config file (env variable)
    :return: Cookie jar
    """
    print(ipaddress)

    url = "http://" + ipaddress + "/VisionCenterApiService/api/Login"
    headers = {'Content-Type': "application/json", 'Cache-Control': "no-cache"}
    payload = {"Login": username, "Password": password}
    payload = str(payload)
    s = requests.Session()
    s.headers.update()
    logger.debug("ip: " + ipaddress + " username: " + username + " passw: " + password)
    try:
        s.post(url, data=payload, headers=headers)

    except ConnectionError as err:
        logger.error("sautervision_login: " + str(err))
    except TimeoutError as err:
        logger.error("sautervision_login: " + str(err))
    except Exception as err:
        logger.error("sautervision_login: " + str(err))
    else:
        sautervision_login.jar = s.cookies  # store cookie jar for auth next request
        logger.info('Successful login to SauterVision at: {}'.format(ipaddress))
        return sautervision_login.jar


def sautervision_sensorlist(logger, ipaddress=str, jar=str):
    """
    Get request for dynamic sensorlist from API
    :param logger: Energima logger
    :param ip: Ip address for API
    :param jar: Cookie jar for authorization
    :return: Sautervision_sensorlist.data
    """
    url = "http://" + ipaddress + "/VisionCenterApiService/api/DataObjectList"
    headers = {"Content-Type": "application/json", 'Cache-Control': "no-cache"}
    querystring = {
        "options.type": "0",
        "options.value": "1",
        "options.includeOnlyEmm": "False",
        "options.includeInactive": "True",
        "options.includeAutomatic": "True",
        "options.pageNumber": "1",
        "options.itemsPerPage": "100"}
    try:
        request = requests.get(url, cookies=jar, headers=headers, params=querystring)
    except ConnectionError as err:
        logger.error("sautervision_sensorlist: " + str(err))
    except TimeoutError as err:
        logger.error("sautervision_sensorlist: " + str(err))
    except Exception as err:
        logger.error("sautervision_sensorlist: " + str(err))
    else:
        sautervision_sensorlist.data = request.json()
        return sautervision_sensorlist.data


def local_sensorlist(logger):
    """
    local cleaned sensorlist. for failsafe if no sensorlist run request to API for dynamic list
    :param logger: Energima logger
    :return: sensorlist
    """
    try:
        with open('active_sensors.json', 'r') as sensor_list:
            sensors = json.load(sensor_list)
            sensor_list.close()
            logger.debug(sensors)
    except FileNotFoundError as err:
        logger.error("local_sensorlist: " + str(err))
        logger.warning("Loading sensor list from SauterVision as failsafe...")
        sensors = sautervision_sensorlist(logger, ipaddress=ip, jar=jar_main)
        local_sensorlist.sensors = sensors
        return local_sensorlist.sensors
    else:
        logger.info("Local sensor list loaded")
        local_sensorlist.sensors = sensors
        return local_sensorlist.sensors


def ticks(dt):
    """
    Converts data time format into ticks from ms from 01.01.1970
    :param dt :
    :return
    """
    now = datetime.now()  # date and time now, for system time zone
    t0 = datetime(1, 1, 1)
    test = dt
    return (dt - t0).total_seconds() * 10 ** 7


def unix_to_ticks(t_unix):
    """
    Unix to ticks converter
    """
    t_from = str(pandas.to_datetime(t_unix, unit="ms"))  # from unix to timestamp
    t_from_ed = t_from[:-5]
    # unix_to_ticks.ticks_from = float_to_str(ticks(datetime.strptime(t_from_ed, "%Y-%m-%d %H:%M:%S")))
    unix_to_ticks.ticks_from = datetime.strptime(t_from_ed, "%Y-%m-%d %H:%M:%S.%f")
    # print(unix_to_ticks.ticks_from)
    return unix_to_ticks.ticks_from


def float_to_str(f):
    """
    Convert the given float to a string,
    without resorting to scientific notation
    """
    ctx = decimal.Context()
    ctx.prec = 25
    d1 = ctx.create_decimal(repr(f))
    return format(d1, 'f')


def sautervision_data(logger, sensor_id, ipaddress=str, jar=str, timenow=str, timepast=str, pagenr=str):
    """Get request to API, data for a given time interval, answer is max 100 lines/ paginated
    :param logger: Energima logger
    :param ipaddress: IP for API
    :param jar: Cookie jar from login
    :param timenow: time local now
    :param timepast: how far back you want data
    :param sensorid: sensor id for singel datapoint
    :param pagenr: How many pages of data
    :return sautervision_data.no_pages
      """
    from_ticks = float_to_str(ticks(timepast))
    to_ticks = float_to_str(ticks(timenow))
    sensor_id_str = str(sensor_id)
    ip = str(ipaddress)
    logger.debug(ip, jar)

    url2 = "http://" + ip + "/VisionCenterApiService/api/Historicaldata"
    headers = {"Content-Type": "application/json", 'Cache-Control': "no-cache"}
    querystring = {
        "options.objectId": sensor_id_str,
        "options.dateFrom": from_ticks,
        "options.dateTo": to_ticks,
        "options.itemsPerPage": "100",
        "options.pageNumber": pagenr}
    try:
        r = requests.get(url2, cookies=jar, headers=headers, params=querystring)
    except ConnectionError as err:
        logger.error("sautervision_data: " + str(sensor_id_str) + str(err))
    except RuntimeError as err:
        logger.error("sautervision_data: " + str(sensor_id_str) + str(err))
    except Exception as err:
        logger.error("sautervision_data: " + str(sensor_id_str) + str(err))
    else:
        sautervision_data.no_pages = r.json()
        return sautervision_data.no_pages


def sautervision_dataprocessing_hist(logger, sensor_id, ipaddress=str):
    """
    function for looping trough pagination and setup for timeinterval
    :param logger: Energima logger
    :param ipaddress: IP for API
    :param sensorid: sensor id for singel datapoint
    :return sautervison_dataprocessing.data
    """
    t_now = datetime.now()
    d1 = timedelta(days=1, hours=2, minutes=0, seconds=0,
                   microseconds=0)  # Default delta_t if noe "last timestamp" found
    t_past = t_now - d1

    page_no = 1  # page_no = 1 for use only first time to fetch page_count
    response = sautervision_data(logger, sensor_id,
                                 ipaddress=ipaddress,
                                 jar=sautervision_login.jar,
                                 timenow=t_now,
                                 timepast=t_past,
                                 pagenr=page_no)

    tot_pages = response["PageCount"]
    if tot_pages != 1:
        try:
            for page_no in range(2, tot_pages + 1):

                data = sautervision_data(logger, sensor_id,
                                         ipaddress=ipaddress,
                                         jar=sautervision_login.jar,
                                         timenow=t_now,
                                         timepast=t_past,
                                         pagenr=page_no)
                response["HistoricalDataValues"].extend(data["HistoricalDataValues"])
        except ConnectionError as err:
            logger.error("sautervision_dataprocessing_hist: " + str(err))
        except RuntimeError as err:
            logger.error("sautervision_dataprocessing_hist: " + str(err))
        except Exception as err:
            logger.error("sautervision_dataprocessing_hist: " + str(err))
        else:
            sautervision_dataprocessing_hist.data = response
            logger.debug("values from pages: {} of {}".format(page_no, tot_pages) + " for Id: " + str(sensor_id))
            return sautervision_dataprocessing_hist.data
    else:
        logger.debug("Importing values from single page for Id: " + str(sensor_id))
        sautervision_dataprocessing_hist.data = response
        return sautervision_dataprocessing_hist.data


def sautervision_dataprocessing_live(logger, sensor, ipaddress=str):
    """
    Starts request for live data retrieval.
    :param logger: Energima logger
    :param ipaddress: Ip address for API
    :param sensor: Sensor id from sensorlist
    :return data: Live data value from sensor
    """

    ipaddress_str = str(ipaddress)
    sensor_id_str = str(sensor)

    url2 = "http://" + ipaddress_str + "/VisionCenterApiService/api/DataObject"
    headers = {"Content-Type": "application/json", 'Cache-Control': "no-cache"}
    querystring = {"options.objectId": sensor_id_str, "options.propertyId": "85", }
    try:
        r = requests.get(url2, cookies=sautervision_login.jar, headers=headers, params=querystring)
    except ConnectionError as err:
        logger.error("sautervision_dataprocessing_live: " + str(err) + "Download failed for ID: " + sensor_id_str)
    except RuntimeError as err:
        logger.error("sautervision_dataprocessing_live: " + str(err))
    except Exception as err:
        logger.error("sautervision_dataprocessing_live: " + str(err))
    else:
        sautervision_dataprocessing_live.data = r.json()
        return sautervision_dataprocessing_live.data
