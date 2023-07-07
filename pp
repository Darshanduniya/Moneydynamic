from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pymonetdb
import subprocess
import logging

default_args = {
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    "UK_HANDSHAKE",
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2023, 5, 14),
    catchup=False,
)

def read_industry_names(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file]

def read_industries(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file]

def read_monetdb_credentials(filename):
    credentials = {}
    with open(filename, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            credentials[key] = value
    return credentials

# File paths
industry_names_file = '/path/to/industry_names.txt'
industries_file = '/path/to/industries.txt'
monetdb_credentials_file = '/path/to/monetdb_credentials.txt'

# Read industry names and industries from files
industry_names = read_industry_names(industry_names_file)
industries = read_industries(industries_file)

# Read MonetDB connection credentials from file
monetdb_credentials = read_monetdb_credentials(monetdb_credentials_file)

# Weekly job and monthly job lists
weekly_jobs = ['tec_pos_ikw', 'tec_pos_osw', 'apl_pos_opm', 'apl_pos_stm']
monthly_jobs = ['tec_pos_cet', 'tec_pos_bea']

def execute_monet_query(industry_name, industry, monetdb_credentials):
    conn = pymonetdb.connect(
        username=monetdb_credentials['username'],
        password=monetdb_credentials['password'],
        hostname=monetdb_credentials['hostname'],
        port=int(monetdb_credentials['port']),
        database=monetdb_credentials['database']
    )
    cursor = conn.cursor()

    query = f'''select count(*) from "execution_log" where operation_type='ab' and job_status='SUCCESS' and sys.str_to_date("execution_end_time",'%y-%m-%d')=current_date AND industry_name='{industry_name}' and flag='0' '''
    cursor.execute(query)

    results = cursor.fetchone()[0]
    results = int(results)

    query_two = f'''select offline_mart from "LATES_REFRESH_DETAILS" WHERE industry='{industry}' '''
    cursor.execute(query_two)

    result_two = cursor.fetchone()[0]
    print(result_two)

    if results > 0:
        if industry_name in weekly_jobs:
            job_type = 'weekly'
            print(f"{industry_name} is a weekly job")
        elif industry_name in monthly_jobs:
            job_type = 'monthly'
            print(f"{industry_name} is a monthly job")
        else:
            job_type = 'unknown'
            print(f"Unknown job type for {industry_name}")

        if results > 0 and result_two == 'B':
            if job_type == 'weekly':
                dag_id = f'{industry_name}_B'
                subprocess.run(['airflow', 'dags', 'trigger', dag_id])

                update_query = f'''UPDATE "execution_log" SET dag_flag=1 WHERE operation_type='ab' and job_status='SUCCESS' and sys.str_to_date("execution_end_time",'%y-%m-%d')=current_date AND industry_name='{industry_name}' AND dag_flag=0 '''
                cursor.execute(update_query)

                update_query_two = f'''UPDATE "LATES_REFRESH_DETAILS" SET offline_mart='A', online_mart='B' where industry='{industry}' '''
                cursor.execute(update_query_two)
                conn.commit()

                # Pause the DAG until the next Monday
                next_monday = datetime.now() + timedelta(days=(7 - datetime.now().weekday()) % 7)
                print("Next Monday:", next_monday.strftime("%Y-%m-%d"))
                dag_paused = True
                next_monday = datetime.now() + timedelta(days=(7 - datetime.now().weekday()) % 7)
                dag.add_task_instance_pause(next_monday)

            elif job_type == 'monthly':
                dag_id = f'{industry_name}_B'
                subprocess.run(['airflow', 'dags', 'trigger', dag_id])

                update_query = f'''UPDATE "execution_log" SET dag_flag=1 WHERE operation_type='ab' and job_status='SUCCESS' and sys.str_to_date("execution_end_time",'%y-%m-%d')=current_date AND industry_name='{industry_name}' AND dag_flag=0 '''
                cursor.execute(update_query)

                update_query_two = f'''UPDATE "LATES_REFRESH_DETAILS" SET offline_mart='A', online_mart='B' where industry='{industry}' '''
                cursor.execute(update_query_two)
                conn.commit()

                # Pause the DAG until the 1st of the next month
                first_of_next_month = datetime.now().replace(day=1, month=datetime.now().month % 12 + 1, year=datetime.now().year + (datetime.now().month + 1) // 12)
                print("1st of Next Month:", first_of_next_month.strftime("%Y-%m-%d"))
                dag_paused = True
                first_of_next_month = datetime.now().replace(day=1, month=datetime.now().month % 12 + 1, year=datetime.now().year + (datetime.now().month + 1) // 12)
                dag.add_task_instance_pause(first_of_next_month)

        elif results > 0 and result_two == 'A':
            dag_id = f'{industry_name}_A'
            subprocess.run(['airflow', 'dags', 'trigger', dag_id])

            update_query = f'''UPDATE "execution_log" SET dag_flag=1 WHERE  operation_type='ab' and job_status='SUCCESS' and sys.str_to_date("execution_end_time",'%y-%m-%d')=current_date AND industry_name='{industry_name}' AND dag_flag=0 '''
            cursor.execute(update_query)

            update_query_two = f'''UPDATE "LATES_REFRESH_DETAILS" SET offline_mart='B', online_mart='A' where industry='{industry}' '''
            cursor.execute(update_query_two)
            conn.commit()

    else:
        print("validation issue")

    cursor.close()
    conn.close()

# Create tasks dynamically
for industry_name, industry in zip(industry_names, industries):
    execute_query_task = PythonOperator(
        task_id=f'execute_monetdb_query_{industry_name}',
        python_callable=execute
