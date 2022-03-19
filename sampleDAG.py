from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator


def failureNotification(context):
    dag_run = context.get('dag_run')
    EmailOperator(
        task_id = "send_email",
        to='is3107.g7@gmail.com', 
        subject=f"DAG {dag_run} has failed - Fatal", 
        html_content=""" <h3> Your Dag has failed 5 times. DAG Failure fatal. </h3> """,
        dag=dag)

def retryNotification(context):
    dag_run = context.get('dag_run')
    EmailOperator(
        task_id = "send_email",
        to='is3107.g7@gmail.com', 
        subject=f"DAG {dag_run} has failed - Retrying", 
        html_content=""" <h3> Your Dag has failed. We are trying it again </h3> """,
        dag=dag)


with DAG(
    'DAGNAME',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['is3107.g7@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        'on_failure_callback': failureNotification,
        #'on_success_callback': successNotification,
         'on_retry_callback': retryNotification,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='DESCRIPTION OF DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['SOMETAG'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3]

