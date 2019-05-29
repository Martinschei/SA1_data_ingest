import argparse
import yaml
import sys
import os
import logging
import datetime
from datetime import datetime
from dateutil import parser

# Cogntie imports
from cognite_logger import cognite_logger
from cognite.v05 import timeseries
from cognite.v05.raw import RawRow
from cognite.v05.dto import Datapoint
from cognite_uploader import cognite_uploader

# test import
from SauterAPI_module_v3_2 import ticks, float_to_str, unix_to_ticks

from __main__ import *


def get_last_timestamp(cdp_ts_name, project_name, api_key):
    # cdp_ts_name = "310_01-RT402"
    get_last_timestamp.res = timeseries.get_latest(cdp_ts_name, api_key=api_key, project=project_name)

    return get_last_timestamp.res


def get_parser():
    get_parser.argument_parser = argparse.ArgumentParser(
        description='Simple util to populate a RAW table based on a configuration'
    )
    get_parser.argument_parser.add_argument(
        '--config',
        help='The config.yml file containing configuration information for the connection to CDP and input parameters'
    )
    return get_parser.argument_parser


def upload_objects(objects, api_key, project_name, server, upload_bytes_threshold, log):
    separator = "_"
    data = []
    for obj in objects:
        obj["original_name"] = obj["Name"][:]
        obj["name"] = obj["Name"].replace(".", separator)
        del obj["Name"]
        if "analog" in obj["ObjectType"] or "binary" in obj["ObjectType"]:
            obj["type"] = "numerical"
        else:
            obj["type"] = "string"
        data.append(RawRow(obj["name"], obj))

    log.info("Will upload " + str(len(data)) + " objects.")

    uploader = cognite_uploader.Uploader(api_key=api_key, project=project_name, base_url=server,
                                         post_upload_function=None, queue_threshold=upload_bytes_threshold)
    # uploader.prepare_databases(raw_db, raw_table)
    for row in data:
        print(cdp_startup.raw_db)
        uploader.add_to_upload_queue(cdp_startup.raw_db, cdp_startup.raw_table, row)
    uploader.upload()


def upload_datapoints_historical(logger, sensor_id, sensor_data, api_key, project_name, log):
    """Historical datapoints uploader, unique for Sauter Vision API
    Converts timestamp to unix and batches data if over 10000 and upload to RAW
    :param logger: Energima logger
    :param sensor_data: data from API client
    :param api_key: API key from CDP
    :param project_name: Energima
    :return None
    """
    sensor_values = sensor_data["HistoricalDataValues"]
    var1 = sensor_id["Name"].replace(".", "_")
    name = var1.replace(" ", "_")

    points = []
    for object in sensor_values:
        t = object["LocalTimestamp"]
        o = object["UtcOffset"]
        hours = round(o/60)
        minutes = o - hours*60
        if hours < 0:
            utcstring = '-'
        else:
            utcstring = '+'
        if 9.5>hours>-9.5:
            utcstring += '0'
            utcstring += str(abs(hours))
        else:
            utcstring+= str(abs(hours))
        utcstring += ':'
        if 9.5>minutes>-9.5:
            utcstring += '0'
            utcstring += str(abs(minutes))
        else:
            utcstring += str(abs(minutes))
        t += utcstring
        timestamp = int(parser.parse(t).timestamp()*1000)
        val = float(object["ReceivedValue"].replace(",", "."))
        points.append(Datapoint(timestamp, val)

        if len(points) >= 10000:  # Post in batches of 10K
            try:
                timeseries.post_datapoints(name, points, api_key=api_key, project=project_name)
                points = []
            except ConnectionError as err:
                logger.error("upload_datapoints_historical: " + str(err))
            except TimeoutError as err:
                logger.error("upload_datapoints_historical: " + str(err))
            except Exception as err:
                logger.error("upload_datapoints_historical: " + str(err))
            else:
                log.info("Posting a batch")
    try:
        timeseries.post_datapoints(name, points, api_key=api_key, project=project_name)
    except ConnectionError as err:
        logger.error("upload_datapoints_historical: " + str(err))
    except TimeoutError as err:
        logger.error("upload_datapoints_historical: " + str(err))
    except Exception as err:
        logger.error("upload_datapoints_historical: " + str(err))
    else:
        log.info("Posting last data")


def upload_datapoints_live(logger, sensor, api_key, project_name, log):
    """Live datapoints uploader, unique for Sauter Vision API
         Converts timestamp to unix and upload to RAW
        :param logger: Energima logger
        :param sensor_data: data from API client
        :param api_key: API key from CDP
        :param project_name: Energima
        :return None
        """
    sensor_values = sensor
    name = sensor_values["Name"]

    points = []
    t = sensor_values["LocalTimestamp"]

    if "," in (sensor_values["PresentValue"]):
        val = float(sensor_values["PresentValue"].replace(",", "."))
        points.append(Datapoint((int(t * 1000)), val))
    elif sensor_values["PresentValue"] == "inactive":
        val = (sensor_values["PresentValue"])
        points.append(Datapoint((int(t * 1000)), val))
    elif sensor_values["PresentValue"] == "active":
        val = (sensor_values["PresentValue"])
        points.append(Datapoint((int(t * 1000)), val))
    else:
        val = float(sensor_values["PresentValue"])
        points.append(Datapoint((int(t * 1000)), val))
    try:
        timeseries.post_datapoints(name, points, api_key=api_key, project=project_name)
        log.info("Posting last data")
    except ConnectionError as err:
        logger.error("upload_datapoints_historical: " + str(err))
    except TimeoutError as err:
        logger.error("upload_datapoints_historical: " + str(err))
    except Exception as err:
        logger.error("upload_datapoints_historical: " + str(err))
    else:
        log.info("Posting last data")

def cdp_startup(logger):
    parser = get_parser()
    parser.parse_args()

    # Initiate logger
    log = logging.getLogger("input_generator.py")
    # Configure root logger
    cognite_logger.configure_logger(
        logger_name=None,
        log_json=False,
        log_level="INFO", )

    # Configure application logger
    cognite_logger.configure_logger(
        logger_name="input_generator.py",
        log_json=False,
        log_level="INFO", )
    # ------------------------------------------------------------------------------------------------------------------
    # Read and parse configuration
    cdp_startup.configuration = None

    try:
        # with open(args.config, 'r') as stream:
        with open('config_SA.yml', 'r') as stream:
            cdp_startup.configuration = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        log.error("Could not parse the configuration.", exc_info=exc)
        sys.exit(1)

    # read api connection information
    cdp_startup.server = cdp_startup.configuration["cognite"]["server"]
    cdp_startup.project_name = cdp_startup.configuration["cognite"]["project_name"]
    api_key = cdp_startup.configuration["cognite"]["api_key"]
    cdp_startup.api_key = os.getenv(api_key)
    # -------------------------------------------------------------------------------------------------------------------
    # Read input configuration
    cdp_startup.filename = cdp_startup.configuration["input"]["filename"]
    cdp_startup.key = cdp_startup.configuration["input"]["key"]
    # ------------------------------------------------------------------------------------------------------------------
    # Read output configuration
    cdp_startup.upload_bytes_threshold = cdp_startup.configuration["output"]["upload_bytes_threshold"]
    cdp_startup.raw_db = cdp_startup.configuration["output"]["raw_db"]
    cdp_startup.raw_table = cdp_startup.configuration["output"]["raw_table"]
    logger.info(cdp_startup.raw_table)
    logger.info(cdp_startup.raw_db)

    log.info("Starting...")

    # fetching sensor list for timeseries and uploading them to CDP RAW layer
    # Note: An internal pipeline will pick them up, but there is not guarantee of success

    cdp_startup.log = log

    return cdp_startup.project_name, cdp_startup.api_key, cdp_startup.server, cdp_startup.raw_table, cdp_startup.raw_db, \
           cdp_startup.upload_bytes_threshold, cdp_startup.key, cdp_startup.filename, cdp_startup.configuration, cdp_startup.log

