#!/bin/bash

payload=$1
content=${2:-text/csv}

curl --data-binary @${payload} -H "Content-Type: ${content}" -v https://runtime.sagemaker.us-east-1.amazonaws.com/endpoints/DockerEndPoint/invocations
