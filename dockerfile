#Define airflow and python version as arguments
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

# Requirements.txt that contains all the extra packages that we want and don't come with the base airflow image
# Copy requirements.txt from our local directory to the root directory of the docker image
COPY requirements.txt / 

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt