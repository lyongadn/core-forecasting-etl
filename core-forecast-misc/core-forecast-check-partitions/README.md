# core-forecast-check-partitions

Congratulations you have spawned a basic python lambda to deploy to AWS!

This project uses poetry for python package management and virtualenv creation. If you don't already have poetry installed please do so by opening a terminal window and running `pip install poetry`.

1. The first terminal command you will want to run is `poetry config virtualenvs.in-project true`. 
2. Next run `poetry install`. You will notice that a .venv folder will apear in your project root.
3. Packages can be added, removed, or updated with `poetry add pkg-name`, `poetry remove pkg-name`, or `poetry update pkg-name`
4. To enter the virtualenv run `poetry shell`. This virtualenv will contain all required packages for your project.

## Local Testing

This project can be tested localy in two different ways: 1) using your local python environment 2) using a docker lambda environment

### Test with Local Python Environment

This requires Poetry to be installed (ie.- `pip install poetry`)

1. Open terminal and in the project root run `poetry shell`
2. Once in the virtualenv run `python core_forecast_check_partitions/lambda_function.py`
   
### Test with Docker Lambda Environment

This requires docker to be installed and running.

1. Open terminal and in the project root run `./local/lambda_build.sh && ./local/lambda_run.sh`

The `lambda_build.sh` file builds the project using lambda architecture and puts the files in `./dist/src` folder.
The `lambda_run.sh` file runs the project in the `./dist/src` folder using a docker image that is identical to the aws lambda environments that run in the cloud.



## Github Actions

To be able to deploy to your AWS Accounts via github actions you will need to get your repo approved with your account. The following link provides more details on how this can be done: https://cfacorp.atlassian.net/wiki/spaces/EN/pages/684752976/Working+in+AWS+Accounts+from+Github+Actions 

The following workflow files are included for github actions:

- **dev.yaml** : This needs to be triggered manually through the github web interface or cli.  It builds the project, runs unit tests, does a sonar-scan, and deploys the code to the dev aws account. Some teams update the workflow to trigger when a change is pushed to the develop branch.
  
- **test.yaml** : This needs to be triggered manually through the github web interface or cli.  It builds the project, runs unit tests, does a sonar-scan, and deploys the code to the test aws account. Some teams update the workflow to trigger when a change is pushed to the master branch.
  
- **prod.yaml** : This needs to be triggered manually through the github web interface or cli. It deploys to the prod aws account based on desired tag/release created previously by test.yaml.
  
- **pull_request.yaml** : Triggers on pull request to develop or master branches and runs thins such as unit tests and sonar-scanner.

If you would like to extend your github actions workflows please take a look at the following repo: https://github.com/cfacorp/github-actions

An important action that is used in all of the above workflows is the [env-loader](https://github.com/cfacorp/github-actions/tree/master/env-loader). It allows us to dynamically load environment variables from yaml, env, json, or jsonnet files.

One popular action is [slack-send](https://github.com/cfacorp/github-actions/tree/master/slack-send) which allows you to send messages to a desired slack channel at any point during the workflow process.

For additional question please reach out to `#tools-support` on slack.

Thanks!