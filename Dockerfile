# Base image for Airflow
FROM apache/airflow:2.7.1

# Install additional dependencies
RUN pip install --no-cache-dir pandas numpy requests apache-airflow-providers-http

# Copy DAGs to the Airflow DAGs folder
COPY dags/ /opt/airflow/dags/

# Set up entrypoint for Airflow
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["airflow", "standalone"]
