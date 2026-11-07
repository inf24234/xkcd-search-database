FROM marcelmittelstaedt/airflow
COPY airflow-image/startup.sh /startup.sh
COPY dags/ /home/airflow/airflow/dags/
COPY etl/ /home/airflow/airflow/etl/
RUN /home/airflow/.local/bin/pip install pymysql
