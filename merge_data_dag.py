from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#scripts
from consume import merge_data
from datamart_daily_gross_revenue import daily_gross_revenue
from datamart_monthly_gross_revenue_product_level import monthly_gross_revenue_product
from datamart_monthly_orders_category_level import monthly_orders_category_level
from datamart_monthly_orders_city_level import monthly_orders_city_level
from datamart_monthly_orders_product_level import monthly_orders_product_level

default_args = {
    'owner': 'halim',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 20),
    'email': ['halim.iskandar2323@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'merge_data_dag',
    default_args=default_args,
    description='DAG to schedule merge_data function',
    schedule_interval='0 0 * * *'
)

# Create the PythonOperator to execute merge_data
merge_data_task = PythonOperator(
    task_id='merge_data_task',
    python_callable=merge_data,
    dag=dag
)
# Create the PythonOperator for daily_gross_revenue
get_table_a_task = PythonOperator(
    task_id='create_daily_gross_revenue_task',
    python_callable=daily_gross_revenue,
    dag=dag
)

# Create the PythonOperator for monthly_gross_revenue_product
get_table_b_task = PythonOperator(
    task_id='create_monthly_gross_revenue_product_task',
    python_callable=monthly_gross_revenue_product,
    dag=dag
)

# Create the PythonOperator for monthly_orders_category_level
get_table_b_task = PythonOperator(
    task_id='create_monthly_orders_category_level_task',
    python_callable=monthly_orders_category_level,
    dag=dag
)

# Create the PythonOperator for monthly_orders_city_level
get_table_b_task = PythonOperator(
    task_id='create_monthly_orders_city_level_task',
    python_callable=monthly_orders_city_level,
    dag=dag
)

# Create the PythonOperator for monthly_orders_product_level
get_table_b_task = PythonOperator(
    task_id='create_monthly_orders_product_level_task',
    python_callable=monthly_orders_product_level,
    dag=dag
)

# Set the dependencies between tasks
merge_data_task.set_downstream(daily_gross_revenue)
merge_data_task.set_downstream(monthly_gross_revenue_product)
merge_data_task.set_downstream(monthly_orders_category_level)
merge_data_task.set_downstream(monthly_orders_city_level)
merge_data_task.set_downstream(monthly_orders_product_level)