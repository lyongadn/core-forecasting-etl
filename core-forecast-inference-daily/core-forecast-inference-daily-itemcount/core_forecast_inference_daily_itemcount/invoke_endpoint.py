#!/usr/bin/env python

"""This is the lambda function responsible for invoking
a sagemaker endpoint to run inferences"""

import time
import logging
import json
import boto3
SAGEMAKER = boto3.client('sagemaker')
SAGEMAKER_RUNTIME = boto3.client('sagemaker-runtime',region_name='us-east-1')
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def invoke_lstm_endpoint(config):
    """ This method invokes a sagemaker endpoint,
    it fetches parameters to be passed as payload from config object """
    payload = json.dumps(config.get_config())
    invoked_successfully = False
    while not invoked_successfully:
        try:
            SAGEMAKER.get_waiter('endpoint_in_service').wait(
                EndpointName=config.get_endpoint_name())
        finally:
            resp = SAGEMAKER.describe_endpoint(
                EndpointName=config.get_endpoint_name())
            status = resp['EndpointStatus']
            if status == 'InService':
                response = SAGEMAKER_RUNTIME.invoke_endpoint(EndpointName=config.get_endpoint_name(
                ), Body=payload, ContentType="application/json")
                LOGGER.info(response['Body'].read())
                invoked_successfully = True
            else:
                print("Waiting for endpoint to come in service")
                time.sleep(300)
    return "success"

def lambda_handler(config):
    """ This is the method which is invoked first when
    the lambda function is invoked from jenkins """
    invoke_lstm_endpoint(config)
    return "invoked_successfully"
