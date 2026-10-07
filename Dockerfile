FROM apache/airflow:2.11.2-python3.12

USER root
# Install git or any system level dependencies if needed
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt