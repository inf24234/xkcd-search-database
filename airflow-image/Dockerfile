FROM marcelmittelstaedt/airflow
COPY airflow-image/startup.sh /startup.sh
RUN chmod +x /startup.sh
COPY dags/ /home/airflow/airflow/dags/
COPY etl/ /home/airflow/airflow/etl/
RUN pip3 install pymysql

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    HADOOP_USER_NAME=hadoop