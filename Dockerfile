FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir dbt-bigquery==1.7.4

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
