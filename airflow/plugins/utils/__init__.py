from airflow.exceptions import AirflowSkipException


def pre_execute_skip_tasks(context):
    task_id = context["task"].task_id
    conf = context["dag_run"].conf or {}
    skip_tasks = conf.get("skips", []) or conf.get("skip_tasks", [])
    if task_id in skip_tasks:
        raise AirflowSkipException()
