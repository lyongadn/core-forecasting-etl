import boto3

def lambda_handler(event, context):
    client = boto3.client('rds')
    if event['action'] == 'increase_size':
        print(f'increasing the size of RDS')
        response = client.modify_db_instance(
            DBInstanceIdentifier=event['identifier'],
            DBInstanceClass=event['db_instance'],
            ApplyImmediately=True
        )
    else:
        print(f'decreasing the size RDS')
        response = client.modify_db_instance(
            DBInstanceIdentifier=event['identifier'],
            DBInstanceClass=event['db_instance'],
            ApplyImmediately=True
        )