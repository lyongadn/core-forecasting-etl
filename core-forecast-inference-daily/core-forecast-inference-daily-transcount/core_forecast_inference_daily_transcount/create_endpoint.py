#!/usr/bin/env python

""" This is the lambda function responsible for Creating a sagemaker endpoint to run inferences """

import time
import logging
import traceback
import boto3
from botocore.exceptions import ClientError
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
SAGEMAKER = boto3.client('sagemaker')


def create_lstm_model(config):
    """ 
    This method takes config object as parameter and creates a model in sagemaker service
    as per the docker image uri provided in primary container attribute
    """
    created_model = False
    while not created_model:
        try:
            response = SAGEMAKER.create_model(
                ModelName=config.get_model_name(),
                PrimaryContainer={
                    'Image': config.get_docker_image()
                },
                ExecutionRoleArn=config.get_iam_role(),
                VpcConfig={
                    'SecurityGroupIds': [config.get_security_group()],
                    'Subnets': config.get_subnet()
                }
            )
            LOGGER.info("Created Model : "+config.get_model_name())
            created_model = True
        except ClientError:
            LOGGER.warning("encountered error , will try to delete again")
            traceback.print_exc()
            print("sleeping")
            time.sleep(3)


def create_lstm_config(config):
    """
    This method takes config object as parameter and creates a config
    for the sagemaker endpoint by specifying EC2 instance type Instance 
    Count and  Model name
    """
    created_endpoint_config = False
    while not created_endpoint_config:
        try:
            response = SAGEMAKER.create_endpoint_config(
                EndpointConfigName=config.get_endpoint_config_name(),
                ProductionVariants=[
                    {
                        'VariantName': 'version-1',
                        'ModelName': config.get_model_name(),
                        'InitialInstanceCount': 1,
                        'InstanceType': config.get_instance_type()
                    },
                ]
            )
            LOGGER.info("Created Config : "+config.get_endpoint_config_name())
            created_endpoint_config = True
        except ClientError:
            LOGGER.warning("encountered error , will try to delete again")
            traceback.print_exc()
            print("sleeping")
            time.sleep(3)


def create_lstm_endpoint(config):
    """
    This method creates a Sagemaker Endpoint as per the  config
    and lstm model pushed by the above two methods and takes a config object as parameter
    """
    created_endpoint = False
    while not created_endpoint:
        try:
            response = SAGEMAKER.create_endpoint(
                EndpointName=config.get_endpoint_name(),
                EndpointConfigName=config.get_endpoint_config_name()
            )
            LOGGER.info("Created Endpoint : "+config.get_endpoint_name())
            created_endpoint = True
        except ClientError:
            LOGGER.warning("encountered error , will try to delete again")
            traceback.print_exc()
            print("sleeping")
            time.sleep(3)


def lambda_handler(config):
    """ This is the method which gets triggered when the lambda  function is invoked """
    create_lstm_model(config)
    create_lstm_config(config)
    create_lstm_endpoint(config)
    return "Done"