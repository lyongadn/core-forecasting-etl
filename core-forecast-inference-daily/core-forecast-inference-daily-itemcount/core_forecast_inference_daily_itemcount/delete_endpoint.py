#!/usr/bin/env python

"""This is the lambda function responsible for deleting the sagemaker inference
after generating the forecast """

import time
import logging
import traceback
import boto3
from botocore.exceptions import ClientError
SAGEMAKER = boto3.client('sagemaker')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def delete_lstm_model(config):
    """ This method deletes the lstm model in sagemaker service,
    it takes model_name as parameter """
    deleted_model = False
    while not deleted_model:

        try:
            response = SAGEMAKER.delete_model(ModelName=config.get_model_name())
            LOGGER.info("Deleted : " + config.get_model_name())
            deleted_model = True
        except ClientError:
            traceback.print_exc()
            LOGGER.warning("retry to delete model in some time")
            time.sleep(3)


def delete_lstm_config(config):
    """ This method deletes the lstm_model_config created for endpoint,
    it takes EndpointConfigName as parameter passed from config object """
    deleted_endpoint_config = False
    while not deleted_endpoint_config:

        try:
            response = SAGEMAKER.delete_endpoint_config(
                EndpointConfigName=config.get_endpoint_config_name())
            LOGGER.info("Deleted : " + config.get_endpoint_config_name())
            deleted_endpoint_config = True
        except ClientError:
            traceback.print_exc()
            LOGGER.warning("retry to delete endpoint in some time")
            time.sleep(3)


def delete_lstm_endpoint(config):
    """ This method deletes the lstm_model_enpoint which generates forecast created,
    it takes EndpointName as parameter passed from config object """
    deleted_endpoint = False
    while not deleted_endpoint:

        try:
            response = SAGEMAKER.delete_endpoint(
                EndpointName=config.get_endpoint_name())
            LOGGER.info("Deleted : " + config.get_endpoint_name())
            deleted_endpoint = True
        except ClientError:
            traceback.print_exc()
            LOGGER.warning("retry to delete model in some time")
            time.sleep(3)


def lambda_handler(config):
    """ This is the method which gets triggered when the lambda function is invoked """
    delete_lstm_endpoint(config)
    delete_lstm_config(config)
    delete_lstm_model(config)
    return "Done"