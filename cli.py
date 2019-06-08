#!/usr/bin/env python
"""
Main runner for LinkFire.
"""

import json
import click
import logging
import requests


from uuid import UUID
from .settings import RATES_LINK

logger = logging.getLogger(__name__)


# Global streams
raw_streams = []
streams_with_convusdvalue = []
streams_with_deadletters = []


def get_streams(data_file):
    """
    Function to get the list of data streams from a json file

    Args:
        data_file (str): Json data file
    Returns:
        data (list): Complete extracted list of data streams
    """
    logger.debug("Getting streams from the file")
    data = []
    with open(data_file, "r") as read_file:
        for line in read_file:
            data.append(json.loads(line))
    return data


def process_convusdvalue(stream, convvalue):
    """
    Function to process convusdvalue if convvalue exists

    Args:
        stream (dict): Single stream unit
        convvalue (float): Convvalue required to calculate the currency in USD
    """
    if convvalue:
        logger.debug("Processing convusdvalue")
        live_rates = requests.get(RATES_LINK).json()
        new_stream = dict(stream)
        convusdvalue = {k: convvalue * v for k, v in live_rates.items()}
        new_stream["convusdvalue"] = convusdvalue
        streams_with_convusdvalue.append(new_stream)


def process_deadletters(stream):
    """
    Function to create deadletters when stream has an invalid UUID

    Args:
        stream (dict): Single Stream unit
    """
    logger.debug("Processing deadletters")
    try:
        UUID(stream.get("linkid", None), version=4)
    except (TypeError, ValueError):
        streams_with_deadletters.append(stream)


def append_json(file_name, data):
    """
    Function to append data to a json file,
    if file doesn't exist then a new file is created

    Args:
        file_name (str): Name of the json file to be created
        data (dict): Data stream to be written to the json file
    """
    try:
        with open(file_name, "r") as infile:
            file_data = json.load(infile)
        if isinstance(file_data, dict):
            new_data = [file_data, data]
        if isinstance(file_data, list):
            file_data.append(data)
            new_data = file_data
        with open(file_name, "w") as outfile:
            json.dump(new_data, outfile)
    except FileNotFoundError:
        with open(file_name, "w") as outfile:
            json.dump(data, outfile)


def write_json(file_name, data):
    """
    Function to write data to a json file

    Args:
        file_name (str): Name of the json file to be created
        data (list): Data stream to be written to the json file
    """
    with open(file_name, "w") as outfile:
        json.dump(data, outfile)


def process_type(stream):
    """
    Function to create a new file with the type name and add stream records

    Args:
        stream (dict): Stream unit having type info
    """
    logger.debug("Processing type values")
    type_name = stream.get("type", None)
    if type_name:
        append_json(f"{type_name}.json", stream)


def write_new_streams():
    """
    Function to write the convusdvalue and deadletters streams
    """
    if streams_with_convusdvalue:
        write_json("convusdvalues.json", streams_with_convusdvalue)
    if streams_with_deadletters:
        write_json("deadletters.json", streams_with_deadletters)


def process_streams(streams):
    """
    Function to process the streams as per the requirements

    Args:
        streams (list): List of data streams
    """
    logger.debug("Processing Streams")
    for stream in streams:
        process_convusdvalue(stream, stream.get("convvalue", None))
        process_deadletters(stream)
        process_type(stream)
        write_new_streams()


@click.command()
@click.argument("data_file")
def main(data_file):
    """
    Main function which reads the streams and processes it to achieve the following:
    1) Whenever there is a convvalue, use the rates.json and convvalueunit to convert the value to USD.
    Write that value to a new field convusdvalue.
    2) Split records by values in the type field and create one output file for each type.
    3) Validate that the linkid is a valid UUID, using the standard library.
    Invalid records must go to their own output file, e.g. "deadletters.json".

    Args:
        data_file (str): Data Streams file
    """
    logger.info(f"Processing data from {data_file}")
    streams = get_streams(data_file)
    process_streams(streams)
