import airflow
from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'Charizard',
    'start_date': airflow.utils.dates.days_ago(1),
    'dataflow_default_options': {
        'project': Variable.get('project_ids'),
        'location': Variable.get('location'),
        'tempLocation': Variable.get('gcs_basepath') + '/temp',
    }
}

dag = DAG(
    'pokemon_dataflow_pipeline',
    default_args=default_args,
    schedule_interval=None,
)

pokemon_dataflow_task = DataFlowPythonOperator(
    task_id='dfp_pokemon_totals_by_type_task',
    py_file= Variable.get('gcs_basepath') + '/dags/dfp_pokemon_totals_by_type.py',
    options={
        'project': Variable.get('project_ids'),
        'region': Variable.get('location'),
    },
    dag=dag,
)
