"""
Домашнее задание по AirFlow для стажеров
"""

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import DAG 
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import logging
import os
import pathlib

default_args = {
	'owner': 'Yanakov',
	'start_date': datetime(2023, 5, 12, 0, 0),
    'end_date': datetime(2023, 5, 20, 0, 0),
	'email': ['dmitriyyanakov@gmail.com'],
	'catchup': False
}
   
with DAG(
	default_args=default_args,
	dag_id='yanakov',
	tags=['examples'],
	start_date= datetime(2023, 5, 12),
	schedule_interval=timedelta(days=1)
) as dag:
	pg_hook = PostgresHook(
		postgres_conn_id='postgres'
		, schema='postgres'
	)
	pg_conn = pg_hook.get_conn()
	cursor = pg_conn.cursor()

	def drop_tbl(**kwargs):
		sql_drop = '''
		drop table if exists postgres.public.yanakov_tab; 
		commit;
		'''
		cursor.execute(sql_drop)

		today_date = kwargs['ds']
		logging.info(f'''Удалена таблица за дату: {today_date}. ''')

	def create_tbl(**kwargs):
		#Создаем таблицу и sequence
		sql_create = '''
		create table if not exists postgres.public.yanakov_tab
			(id  serial primary key, 
			surname varchar(32), 
			university text, 
			grad_year smallint,
			t_changed_dttm timestamp); 
		commit; 
		'''
		cursor.execute(sql_create)

	def insert_from_airflow(**kwargs):
		# Запуск sql скрипта напрямую из airflow
		sql_from_airflow = ''' 
		insert into postgres.public.yanakov_tab(surname, university, grad_year, t_changed_dttm) 
		values 
		('Yanakov', 'Lomonosov MSU', 2023, NOW()),
		('Musk', 'Stanford', 1995, NOW()),
		('Gates', 'Harvard', 1975, NOW()),
		('Brin', 'Stanford', 1995, NOW());
		commit; 
		'''
		cursor.execute(sql_from_airflow)

# Определение Tasks (Задач - вершин дага)
	task_drop_tbl = PythonOperator(
		task_id='task_drop_tbl',
		python_callable=drop_tbl,
		do_xcom_push=True
		)

	task_create_tbl = PythonOperator(
		task_id='task_create_tbl',
		python_callable=create_tbl,
		do_xcom_push=True
		)

	task_insert_from_airflow = PythonOperator(
		task_id='task_insert_from_airflow',
		python_callable=insert_from_airflow,
		do_xcom_push=True
		)

# Определение ацикличного графа
	task_drop_tbl >> task_create_tbl >> task_insert_from_airflow
