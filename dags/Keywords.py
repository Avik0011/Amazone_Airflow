from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from Keywords_Helper import keywords_helper
from Keywords_Helper import  keywords_extended_helper
default_args ={
    'owner':'Airflow',
    'start_date': datetime(2021,12,30),
    'retries':1,
    'retry_delay':timedelta(seconds=5)
}
# if catchup false then this means no previous DAG will run
dag = DAG('store_dag',default_args=default_args,schedule_interval='@daily',catchup=False)

# Check file exists or not shasum return true if file exists other wise give error
t1 = PythonOperator(task_id='Get_Data_from_Amazon_Api_keywords',python_callable=keywords_helper,dag=dag)

t2 = PythonOperator(task_id='Get_Data_from_Amazon_Api_keywords_extended',python_callable=keywords_extended_helper, dag=dag)

t1 >> t2