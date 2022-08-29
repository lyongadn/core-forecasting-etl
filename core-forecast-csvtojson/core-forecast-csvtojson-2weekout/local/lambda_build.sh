#!/bin/bash

# This file will build all required packages using lambda architecture and place in the dist directory. 
# Result will be a folder with the src (dist/src) and a zip file (in ./dist) to upload.

# Build for lambda env
mkdir -p ./dist/src;
docker run --rm --workdir /github/workspace -v "${PWD}":/github/workspace --entrypoint /bin/bash lambci/lambda:build-python3.8 -c "\
    pip install poetry --cache-dir=.pip; \
    poetry export -f requirements.txt  -o requirements.txt --without-hashes; \
    pip install -t dist/src -r requirements.txt --cache-dir=.pip ; \
    cp -r core_forecast_csvtojson_twoweekout dist/src/; \
    rm requirements.txt;"
cd dist/src || exit
TIMESTAMP=$(date +"%s")
zip -r "../lambda-pkg-${TIMESTAMP}.zip" ./*
