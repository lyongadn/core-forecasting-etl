#!/bin/bash

# This file runs the code in ./dist/src in a docker lambda environment.
# It also maps your local aws credentials for use with any aws cli or sdk calls.

# AWS Credential Profile to use
AWS_PROFILE=default

# Run in lambda env
docker run --rm -e "AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile ${AWS_PROFILE})" \
  -e "AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile ${AWS_PROFILE})" \
  -e "AWS_SESSION_TOKEN=$(aws configure get aws_session_token --profile ${AWS_PROFILE})" \
  -v "${PWD}/dist/src":/var/task lambci/lambda:python3.8 core_forecast_qc_ratio.lambda_function.lambda_handler \
    '{"name": "hello"}';
