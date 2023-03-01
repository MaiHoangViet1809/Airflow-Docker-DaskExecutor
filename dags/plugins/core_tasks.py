from typing import List, Callable, Dict, cast, Union
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3Hook
from airflow.sensors.base import PokeReturnValue

from airflow.exceptions import AirflowFailException, AirflowSkipException, AirflowException

from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

import os

# -------------------------------------------------------------------------------------------------------------------------------------
BUCKET_NAME = os.env["S3_BUCKET"]
SINK_PATH   = os.env["S3_SINK_PATH"]
SINK_RAW_PATH = os.env["S3_RAWDATA_PATH"]


# -------------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------------------
# PARAMETER HANDLE
@task
def configure_rundate():
    ctx = get_current_context()
    run_dates = ctx["params"].get("RUN_DATES", None)

    if run_dates:
        if type(run_dates) == list:
            pass
        elif type(run_dates) == dict:
            start_date = run_dates.get("start_date", None)
            end_date = run_dates.get("end_date", None)
            if start_date:
                if end_date:
                    if end_date < start_date:
                        raise AirflowFailException("[params] run_dates - start_date > run_date")
                    else:
                        curr_date = datetime.strptime(start_date, '%Y-%m-%d')
                        end_date = datetime.strptime(end_date, '%Y-%m-%d')
                        list_date = []
                        while curr_date <= end_date:
                            list_date += [curr_date.strftime('%Y-%m-%d')]
                            curr_date += timedelta(days=1)
                        run_dates = list_date
                else:
                    raise AirflowFailException("[params] run_dates - missing end_date in dict")
            else:
                raise AirflowFailException("[params] run_dates - missing start_date in dict")
        else:
            run_dates = [run_dates]
    else:
        run_dates = [ctx["logical_date"].strftime('%Y-%m-%d')]
    return run_dates


@task
def transform_rundate(run_date):
    return [{"run_date": run_date}]


@task
def get_logical_date(adjust_days: int = 0):
    ctx = get_current_context()
    return (ctx["logical_date"] + relativedelta(days=adjust_days)).strftime("%Y-%m-%d")


@task
def get_params():
    ctx = get_current_context()
    params = ctx["params"]
    return params


@task
def jinja_render(msg: dict):
    ctx = get_current_context()
    t = ctx["task"]
    _rendered_dict = {k: t.render_template(v, context=ctx) for k, v in msg.items()}
    return _rendered_dict


@task
def eval_str_macro(f_string: str):
    ctx = get_current_context()
    params = ctx["params"]
    logical_date = ctx["logical_date"]
    return eval(f'f"{f_string}"')


# -------------------------------------------------------------------------------------------------------------------------------------
@task
def generate_temp_name(name: str):
    import random
    temp_table_name = name.upper() + "_" + str(random.randint(100000, 999999)) + "_TMPRF"
    return temp_table_name


# -------------------------------------------------------------------------------------------------------------------------------------
# DEPENDENCY HANDLE
@task.sensor(timeout=60 * 60 * 12,
             poke_interval=600,
             mode="reschedule")
def check_path_s3(s3_key: str, bucket_name: str = None, conn_id: str = "S3_default", wildcard_match: bool = False, jinja_key_value: dict=None):
    if s3_key == "":
        return PokeReturnValue(is_done=True, xcom_value=f"s3://{bucket_name}/{s3_key}".replace("_SUCCESS", ""))

    print("s3_key:", s3_key)
    # apply jinja template:
    if jinja_key_value:
        if type(jinja_key_value) == list:
            jinja_key_value = jinja_key_value[0]
        print("jinja_key_value:", jinja_key_value)
        from .core_functions import jinja_render
        bucket_name = jinja_render(source=bucket_name, **jinja_key_value) if bucket_name else bucket_name
        s3_key = jinja_render(source=s3_key, **jinja_key_value)

    bucket_name, key = S3Hook.get_s3_bucket_key(bucket_name, s3_key, 'bucket_name', 'bucket_key')
    source_s3 = S3Hook(aws_conn_id=conn_id)
    print(f'Poking for key : s3://{bucket_name}/{key}')

    if wildcard_match:
        import re
        import fnmatch
        prefix = re.split(r'[\[\*\?]', key, 1)[0]
        keys = source_s3.get_file_metadata(prefix, bucket_name)
        key_matches = [k for k in keys if fnmatch.fnmatch(k['Key'], key)]
        if len(key_matches) == 0:
            return PokeReturnValue(is_done=False)
    else:
        result = source_s3.check_for_key(bucket_name=bucket_name,
                                         key=s3_key)
        if not result:
            PokeReturnValue(is_done=False)

    return PokeReturnValue(is_done=True, xcom_value=f"s3://{bucket_name}/{s3_key}".replace("_SUCCESS", ""))


@task
def preprocess_dependency(config) -> list:
    return list(config.get("DEPENDENCIES", {"": ""}).values())


@task.sensor(poke_interval=60,
             timeout=172800,
             mode="reschedule",
             trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, )
def logic_dependency(table_name: str, list_run_date: list, dag_id: str = "DAG_ID_CENTRALIZE") -> PokeReturnValue:
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import State
    # from airflow.utils import timezone

    ctx = get_current_context()
    ti = ctx['ti']
    force_dependency = ctx["params"].get("force_dependency", "false")

    logical_date = ctx["logical_date"].replace(hour=1, minute=30, second=0, microsecond=0)  # + timedelta(days=-1)
    if logical_date in list_run_date or force_dependency == "true":
        if table_name:
            # timezone.make_aware
            print(dag_id)
            print(table_name)
            print(logical_date)
            dag_run_target = DagRun.find(dag_id=dag_id,
                                         execution_date=logical_date)
            for dr in dag_run_target:
                print(dr)
                ti = dr.get_task_instance(task_id=table_name)
                ti_state = ti.state
                print(f"Task {table_name} status: {ti_state}")
                if ti_state == State.SUCCESS:
                    return PokeReturnValue(is_done=True, xcom_value=table_name)
            return PokeReturnValue(is_done=False, xcom_value="not done yet")
        else:
            raise AirflowSkipException("No Table to wait")

            # bypass dependency if logical date of this TI not in list of target execute date
    else:
        return PokeReturnValue(is_done=True, xcom_value=table_name)


# -------------------------------------------------------------------------------------------------------------------------------------
# LOGGING
@task
def get_log():
    import time
    time.sleep(15)

    ctx = get_current_context()
    ti = ctx["ti"]
    logical_date = ctx["logical_date"]

    tis_dagrun = ti.get_dagrun().get_task_instances()
    tis_dagrun = [ti.__dict__ for ti in tis_dagrun]
    tis_dagrun = sorted(tis_dagrun, key=lambda e: f'{e["operator"]}, {e["state"]}, {e["task_id"]}')

    result = [{"DAG_ID": tis["dag_id"],
               "TASK_ID": tis["task_id"],
               "OPERATOR": tis["operator"],
               "STATE": tis["state"] or "",
               "LOGICAL_DATE": logical_date.strftime("%Y-%m-%d %H:%M:%S"),
               "START_DATE": (tis["start_date"] + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S") if tis.get("start_date", None) else "",
               "END_DATE": (tis["end_date"] + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S") if tis.get("end_date", None) else "",
               "DURATION": round(tis["duration"] or 0, 2),
               "TRY_NUMBER": tis['_try_number'] or 0,
               } for tis in tis_dagrun]

    return result


# -------------------------------------------------------------------------------------------------------------------------------------
# CORE TASK
@task(retries=3, retry_delay=timedelta(minutes=15), max_active_tis_per_dag=6,)
def _base_task(pipe_msg: list, task_params: dict, pipe_callable=None):
    ctx = get_current_context()
    ti = ctx["ti"]
    t = ctx["task"]
    task_params = {k: t.render_template(v, context=ctx) for k, v in task_params.items()}

    map_index = ti.__dict__.get("map_index", None)
    task_params["params"] = ctx["params"]

    print("pipe_msg:", pipe_msg)
    current_run_date = pipe_msg[-1]["run_date"]

    # base process
    result, result_path = pipe_callable(pipe_msg,
                                        current_run_date=current_run_date,
                                        **task_params)

    output_msg = {"task_id": ti.task_id,
                  "run_date": current_run_date,
                  "map_index": map_index,
                  "status": "FINISH" if result == 0 else "ERROR",
                  "callable": getattr(pipe_callable, '__name__', repr(pipe_callable)),
                  "output_name": result_path,
                  }

    pipe_msg += [output_msg]

    if result == 0:
        return pipe_msg
    else:
        ti.xcom_push(key="return_value", value=pipe_msg)
        raise AirflowException("Error on " + ti.task_id)


# -------------------------------------------------------------------------------------------------------------------------------------
# callable
# -------------------------------------------------------------------------------------------------------------------------------------
def proxy_task(task_params: dict, list_pipe_msg: list, pipe_callable: Union[Callable, str] = None, **task_kwargs):
    if type(pipe_callable) == str:
        import importlib
        mod_core_functions = importlib.import_module("plugins.core_functions")
        pipe_callable = getattr(mod_core_functions, pipe_callable)

    return _base_task.override(**task_kwargs) \
        .partial(task_params=task_params, pipe_callable=pipe_callable) \
        .expand(pipe_msg=list_pipe_msg)

