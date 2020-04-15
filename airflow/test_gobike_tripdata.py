#-*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime, timedelta

connection = HttpHook.get_connection('avkabay1@test-en-0002')
user = connection.login
password = connection.password

default_args = {
    'owner': "avkabay1",
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 15),
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'wait_for_downstream': False,
    'email': ['avkabay1@mts.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

sshHook = SSHHook(ssh_conn_id="avkabay1@test-en-0002")

dag = DAG('test_gobike_tripdata', schedule_interval='0 8 * * 6', default_args=default_args)


kinit = SSHOperator(
    task_id='kinit',
    command = "echo '{1}' | kinit {0}".format(user, password),
    ssh_hook=sshHook,
    dag=dag)

python_path='/data/venv/python2/dq-venv/bin/python2.7'
script_dir='/home/avkabay1/spdevices/etl/test_gobike_tripdata/'
task_raw_script='test_raw_gobike_tripdata.py'
task_ods_script='test_ods_gobike_tripdata.py'
task_dm_station_script='test_dm_station_gobike_tripdata.py'
task_dm_trips_script='test_dm_trips_gobike_tripdata.py'
task_dm_teradata_script='test_dm_teradata_gobike_tripdata.py'

task_raw = SSHOperator(
	task_id="test_raw_gobike_tripdata",
	command = ' '.join([python_path, script_dir + task_raw_script]),
	ssh_hook = sshHook,
	dag=dag)

task_ods = SSHOperator(
	task_id="test_ods_gobike_tripdata",
	command = ' '.join([python_path, script_dir + task_ods_script]),
	ssh_hook = sshHook,
	dag=dag)

task_dm_station = SSHOperator(
	task_id="test_dm_station_gobike_tripdata",
	command = ' '.join([python_path, script_dir + task_dm_station_script]),
	ssh_hook = sshHook,
	dag=dag)

task_dm_trips = SSHOperator(
	task_id="test_dm_trips_gobike_tripdata",
	command = ' '.join([python_path, script_dir + task_dm_trips_script]),
	ssh_hook = sshHook,
	dag=dag)

task_dm_teradata = SSHOperator(
	task_id="test_dm_teradata_gobike_tripdata",
	command = ' '.join([python_path, script_dir + task_dm_teradata_script, '-l', user, '-p', password]),
	ssh_hook = sshHook,
	dag=dag)

kinit >> task_raw >> task_ods >> task_dm_station >> task_dm_trips >> task_dm_teradata

