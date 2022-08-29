# Build an image that can do training and inference in SageMaker
# This is a Python 2 image that uses the nginx, gunicorn, flask stack
# for serving inferences in a stable way.

FROM ubuntu:18.04
RUN apt-get -y update && apt-get install -y --no-install-recommends \ 
         software-properties-common \
         wget \
         python3-pip \
         python3.7 \
         nginx \
         gcc \
         ca-certificates \
         python3-dev
         
    #&& rm -rf /var/lib/apt/lists/*

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN pwd

COPY requirements.txt .

# Here we get all python packages in requirements.txt file
# Likewise, pip leaves the install caches populated which uses
# a significant amount of space. These optimizations save a fair amount of space in the
# image, which reduces start up time.
RUN wget https://bootstrap.pypa.io/get-pip.py && python3.7 get-pip.py && python3.7 -m pip install --upgrade --no-cache-dir pip &&\
    pip3 install --no-cache-dir -r requirements.txt
    

# Set some environment variables. PYTHONUNBUFFERED keeps Python from buffering our standard
# output stream, which means that logs can be delivered to the user quickly. PYTHONDONTWRITEBYTECODE
# keeps Python from writing the .pyc files which are unnecessary in this case. We also update
# PATH so that the train and serve programs are found when the container is invoked.

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PATH="/opt/program:${PATH}"

# Set up the program in the image
COPY decision_trees /opt/program
WORKDIR /opt/program

EXPOSE 80

ENTRYPOINT ["python3"]