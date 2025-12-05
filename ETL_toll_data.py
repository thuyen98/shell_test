from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'John_Doe',                  # dummy name
    'start_date': datetime.today(),       # today
    'email': ['dummy_email@example.com'], # dummy email
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='ETL_toll_data',
    description='Apache Airflow Final Assignment',
    default_args=default_args,
    schedule_interval='@daily',  # runs once daily
    catchup=False
)


unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /path/to/downloaded/data.tar -C /path/to/destination/',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1,2,3,4 /path/to/vehicle-data.csv > /path/to/csv_data.csv",
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f1,2,3 /path/to/tollplaza-data.tsv > /path/to/tsv_data.csv",
    dag=dag
)


extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c 1-2,5-6 /path/to/payment-data.txt > /path/to/fixed_width_data.csv",
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste /path/to/csv_data.csv /path/to/tsv_data.csv /path/to/fixed_width_data.csv > /path/to/extracted_data.csv",
    dag=dag
)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk -F',' '{OFS=\",\"; $4=toupper($4); print}' /path/to/extracted_data.csv > /path/to/staging/transformed_data.csv",
    dag=dag
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
