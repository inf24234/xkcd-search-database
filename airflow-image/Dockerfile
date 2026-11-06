FROM marcelmittelstaedt/airflow
COPY startup.sh /startup.sh
COPY dags/ /home/airflow/airflow/dags
COPY etl/ /home/airflow/airflow/etl/
RUN pip install pymysql
