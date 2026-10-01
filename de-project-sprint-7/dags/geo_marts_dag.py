# geo_marts_dag
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 's31894037',
'start_date':datetime(2022, 1, 1),
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

dag_spark = DAG(
    dag_id = "geo_marts_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

geo_marts = SparkSubmitOperator(
task_id='geo_marts',
dag=dag_spark,
application ='/lessons/scripts/geo_marts.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-01-30", "30", "/user/s31894037/data/geo/events", "/user/s31894037/data/geo", "/user/s31894037/data/analytics/geo_data_d30"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

#geo_marts