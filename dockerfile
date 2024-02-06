# THIS IS NOT BEING USED AS WE ARE USING THE STANDARD AIRFLOW IMAGE
# This Dockerfile is used to build a CUSTOM image for an Airflow container
FROM apache/airflow:latest

# Install the necessary Python libraries
RUN pip install requests pandas sqlalchemy 