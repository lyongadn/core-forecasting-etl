import os
import boto3
import json

def lambda_handler(event, context):

    ENV = os.getenv('ENV')

    with open('core_forecast_invoke_step_function/config.json') as config_params:
        conf = json.load(config_params)[ENV]

    pipeline_name = event['pipeline']
    
    client = boto3.client('stepfunctions')
    response = client.start_execution(
        stateMachineArn=conf[pipeline_name]['step_function_arn']
        #name=PARAMS[ENV]['step_function_name']
    )
    print(f"{conf[pipeline_name]['step_function_name']} Step function invoked sucessfully: {response}")

    return {
        "Status": "200",
        "Message": "Step Function Triggered Successfully"    
    }