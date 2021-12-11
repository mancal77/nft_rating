from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7), datetime.min.time())

default_args = {
       'owner': 'airflow',
       'depends_on_past': False,
       'start_date': seven_days_ago,
       'email': ['airflow@airflow.com'],
       'email_on_failure': False,
       'email_on_retry': False,
       'retries': 1,
       'retry_delay': timedelta(minutes = 5),
}

dag = DAG('NFT', default_args = default_args)
t1 = BashOperator(
     task_id = 'Pre Installations',
     bash_command = 'bash /home/naya/airflow/dags/nft_preparation.sh',
     dag = dag)
t2 = BashOperator(
     task_id = 'Bigquery init',
     bash_command = 'python /home/naya/airflow/dags/bigQuery.py',
     dag = dag)
t3 = BashOperator(
     task_id = 'Web Scrapping',
     bash_command = 'python /home/naya/airflow/dags/rarityUpcoming.py',
     dag = dag)
t4 = BashOperator(
     task_id = 'REST API',
     bash_command = 'python /home/naya/airflow/dags/OpenSea.py',
     dag = dag)
t5 = BashOperator(
     task_id = 'Twitter Enrichment',
     bash_command = 'python /home/naya/airflow/dags/twitter_users_data.py',
     dag = dag)
t6 = BashOperator(
     task_id = 'Twitter Enrichment_statuses',
     bash_command = 'python /home/naya/airflow/dags/twitter_statuses_data.py',
     dag = dag)
t7 = BashOperator(
     task_id = 'Twitter Enrichment_final',
     bash_command = 'python /home/naya/airflow/dags/twits_data.py',
     dag = dag)
t8 = BashOperator(
     task_id = 'Bigquery init',
     bash_command = 'python /home/naya/airflow/dags/pySparkData.py',
     dag = dag)
t9 = BashOperator(
     task_id = 'MySQL init',
     bash_command = 'python /home/naya/airflow/dags/mySQL.py',
     dag = dag)
t10 = BashOperator(
     task_id = 'Kafka pub',
     bash_command = 'python /home/naya/airflow/dags/KafkaProducer.py',
     dag = dag)
t11 = BashOperator(
     task_id = 'Kafaka sub',
     bash_command = 'python /home/naya/airflow/dags/KafkaConsumer.py',
     dag = dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11
