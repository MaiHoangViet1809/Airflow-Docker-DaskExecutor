from airflow import DAG
from airflow.utils.task_group import TaskGroup
import os
from datetime import datetime, timedelta

import plugins.core_tasks as ci
import plugins.core_functions as cf

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]
# ---------------------------------------------------------------------------------------------------------------------
DEPENDENCIES = {
    "some_table": "s3:/{{ params.INPUT_FOLDER }}/some_table/{{ f_date_path(logical_date + relativedelta(days=-1)) }}/_SUCCESS",
    }

PARAMS = {"RUN_DATES": datetime.now().date().strftime("%Y-%m-%d"),
          "INPUT_FOLDER": "/*****",
          "DATA_FOLDER": "/*****",
          "JAR_PATH": "/*****/spark-transform-3.2.1-assembly-1.1-SNAPSHOT.jar",
          "CUT_OFF_DAY": 20,
          "CLASS": {"step_1": "com.some_project.step_1",
                    },
          "DEPENDENCIES": DEPENDENCIES
          }

# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
with DAG(**cf.handle_configure(dag_id=MODULE_NAME,
                               start_date=datetime(2022, 1, 1),
                               catchup=False,
                               schedule=None,
                               params=PARAMS,
                               tags=["PROD"])):
    with TaskGroup(group_id="Configuration") as tg0:
        t_rundate = ci.configure_rundate()
        t_rundate_transform = ci.transform_rundate.expand(run_date=t_rundate)

        t_params = ci.get_params()
        t_msg = ci.jinja_render(t_params)

    with TaskGroup(group_id="Dependencies") as tg1:
        t_check_dependency = ci.check_path_s3.partial(conn_id='aws_default',
                                                      wildcard_match=True) \
                                             .expand(s3_key=ci.preprocess_dependency(t_msg))

    with TaskGroup(group_id="Extract_Features") as tg2:
        step_1 = ci.proxy_task(task_id="step_1",
                                      pipe_callable="f_spark_task",
                                      list_pipe_msg=t_rundate_transform,
                                      task_params={"language": "scala",
                                                   "run_mode": "cluster",
                                                   "java_class": "{{ params.CLASS.step_1 }}",
                                                   "spark_params": [("output", "{{ params.DATA_FOLDER }}/some_path_step_1/[[ run_date | strptime | f_date_path ]]/"),
                                                                    ("input",  ",".join(["{{ params.DEPENDENCIES.some_table.replace('s3:/', '').replace('_SUCCESS', '') }}",
                                                                                         ])),
                                                                    ("args", "[[ f_date(run_date | strptime + relativedelta(days=10)) ]]"),
                                                                    ("seperate_input", "true"),
                                                                    ]},
                                      )

        step_2 = ci.proxy_task(task_id="step_2",
                                   pipe_callable=cf.copy_data,
                                   list_pipe_msg=step_1,
                                   task_params={"source": "{{ params.INPUT_FOLDER }}/some_data_source/{{ f_date_path(logical_date + relativedelta(days=-1)) }}/*",
                                                "sink"  : "{{ params.DATA_FOLDER }}/some_data_sink/[[ run_date | strptime | f_date_path ]]/"})

    with TaskGroup(group_id="Model") as tg3:
        def run_model(pipe_msg, current_run_date, **kwargs):
            pass
        RunModel = ci.proxy_task(task_id="RunModel",
                                 pipe_callable=run_model,
                                 list_pipe_msg=step_2,
                                 max_active_tis_per_dag=4,
                                 task_params={},
                                 )

    # TRADITION DEPENDENCY
    tg0 >> tg1 >> tg2 >> tg3
