import sys
import json
import boto3
import os
import jenkinsapi 
from jenkinsapi.jenkins import Jenkins


def get_secret(conf):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=conf['region_name']
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=conf['secret_key']
        ) 
        secret = get_secret_value_response['SecretString']

    except Exception as e:
        print(f'Error: {e}')
        raise e
        
    return json.loads(secret)

def lambda_handler(event, context):

    ENV = os.getenv('ENV')

    with open('core_forecast_jenkins_trigger/config.json') as config_params:
        conf = json.load(config_params)[ENV]

    pipeline_name = event['pipeline']
    print("Pipeline being triggerd is", pipeline_name)
    
    credentials = get_secret(conf)

    JENKINS_URL = credentials['jenkins_url']
    JENKINS_USERNAME = credentials['jenkins_username'] #sys.argv[2] 
    JENKINS_PASSWORD = credentials['jenkins_token']#sys.argv[3]
    JENKINS_JOB_NAME = conf[pipeline_name] #sys.argv[4]

    jenkins_server = Jenkins(JENKINS_URL,
                            username=JENKINS_USERNAME,
                            password=JENKINS_PASSWORD,
                            useCrumb=True)

    # Crumb is feature used to provide security and authorize the API requests. Value of useCrumb parameter depends on the Jenkins server configurations.
    # If the server does not allow request without crumb, then it returns with 403 exit code stating No valid crumb found in the request.

    jobs = jenkins_server.keys()

    if JENKINS_JOB_NAME in jobs:
        print(f'{JENKINS_JOB_NAME} Job exist and building it.')
        jenkins_server.build_job(JENKINS_JOB_NAME)
    else:
        print('Job does not exist.')

    return {
        "Status": "200",
        "Message": "Jenkins Job Trigger Complete"
        
    }