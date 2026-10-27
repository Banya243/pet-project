FROM apache/airflow:2.11.2-python3.11

COPY requirements.txt /tmp/lab-requirements.txt
RUN pip install --no-cache-dir \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt \
      --requirement /tmp/lab-requirements.txt \
    && pip check
