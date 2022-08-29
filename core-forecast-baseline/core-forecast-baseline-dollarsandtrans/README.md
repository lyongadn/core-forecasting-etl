# core-forecast-baseline-dollarsandtrans

You have spawned a Data Lake Glue ETL job!

## Summary
This project sets up the scaffolding for a Glue ETL job using Spark 2.4 to load data into the Data Lake.

### Additional Steps
Before successfully running a job,
- Set the `logs_bucket` and `dataset_id` in `core_forecast_baseline_dollarsandtrans/etl_job`
- Set up Data Lake API Authorization (see [below](#data-lake-api-authorization))

### Noteworthy files
- `core_forecast_baseline_dollarsandtrans/etl_job.py`: Glue job script
- `core_forecast_baseline_dollarsandtrans/requirements.txt`: Python requirements file
- `aws/cloudformation.yaml`: Cloudformation script run during deployment
- `manifest.jsonnet`: The script version number should be set here
- `test/unit/test_etl_job.py`: Default unit test script
- `core_forecast_baseline_dollarsandtrans/compaction.py`: Compaction job (Template is for Delta Lake only)

### Deployment
GitHub Actions deploys the project using these workflows:

- dev.yaml : Triggers off of push to develop and feature branches and runs build, unit test, sonar-scan, and deployment to dev aws account.
- test.yaml : Triggers off of push to master branch and runs build, unit test, sonar-scan, and deployment to dev aws account.
- prod.yaml : Triggers via manual run from repository and deploys based on desired tag/release created previously by test.yaml.
- pull_request.yaml : Triggers on pull request to develop or master and runs unit tests and sonar-scanner.

### Data Lake API Authorization
Each AWS account needs an Okta client which enables the ETL script to generate a Data Lake Okta bearer token to call Data Lake APIs.
To generate the necessary secrets for your account, 
1. Run this Cloudformation script in your account: https://github.com/cfacorp/cfadl/blob/master/cloudformation/auth_setup.json
2. Set the secret `datalake-svc2svc/{env}-client` with the values given by Identity (contact the Data Lake team if you don't have these secrets)
3. Ask the Data Lake team to add your AWS account number to the Data Lake Notifications SNS Topic

### Data Lake Glue Catalog Access
In the case where you connect to the Glue catalog, here are the AWS account IDs needed:
- `566975719123` (DL Prod)
- `249561172061` (DL Test)