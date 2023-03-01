# from pyspark.sql.functions import col, lit, upper, when, current_date, exp, round as spark_round
# from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType, DecimalType
from dateutil.relativedelta import relativedelta
import os

# -------------------------------------------------------------------------------------------------------------------------------------
S3_KEY = os.env["AWS_KEY"]
S3_SECRET = os.env["AWS_SECRET"]
HIVE_METASTORE_URI = os.env["METASTORE_URI"]
SPARK_CLUSTER_URI = os.env["SPARK_CLUSTER_URI"]
INTERNAL_IP_VM = "" # xxx-xxx-xxx-xxx.us-east-2.compute.internal
SPARK_SUBMIT_PATH = "/apps/spark-3.2.1-bin-hadoop3.2/bin/spark-submit"

SPARK_AA_DEFAULT_CONFIG = {"spark.local.dir": "/data_disk/spark_temp",
                           "spark.ui.showConsoleProgress": "true",
                           "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
                           "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
                           "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
                           "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
                           "spark.executor.memory": "14G",
                           "spark.driver.memory": "4G",
                           "spark.hadoop.fs.s3a.access.key": S3_KEY,
                           "spark.hadoop.fs.s3a.awsSecretAccessKey": S3_SECRET,
                           "spark.hadoop.fs.s3a.secret.key": S3_SECRET,
                           "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                           "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

                           "spark.jars.packages": ",".join(["com.google.guava:guava:30.0-jre",
                                                            "org.apache.hadoop:hadoop-aws:3.2.2",
                                                            "org.apache.hadoop:hadoop-common:3.2.2",
                                                            "org.apache.spark:spark-hadoop-cloud_2.12:3.2.2"]),

                           "spark.dynamicAllocation.enabled": "true",
                           "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                           "spark.dynamicAllocation.minExecutors": 0,
                           "spark.dynamicAllocation.executorIdleTimeout": 60,
                           "spark.dynamicAllocation.cachedExecutorIdleTimeout": 60,
                           "spark.dynamicAllocation.shuffleTracking.timeout": 60,
                           "spark.sql.shuffle.partitions": 128,
                           "spark.scheduler.mode": "FAIR",
                           "spark.scheduler.pool": "production",
                           "spark.cores.max": 60,
                           "spark.executor.cores": 4,

                           "spark.hadoop.fs.s3a.fast.upload": "true",
                           # "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",
                           }


# ULTILITY FUNCTION ---------------------------------------------------------------------------------------------------------------------      
def run_sh(command, debug: bool = False, executable=None):
    from subprocess import Popen, PIPE, STDOUT
    print("[run_sh]", debug, command)
    process = None
    linedata = []
    try:
        process = Popen(command,
                        shell=True,
                        stdout=PIPE,
                        stderr=STDOUT,
                        encoding='utf-8',
                        errors='replace',
                        executable=executable or '/bin/bash')
        while True:
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            if realtime_output:
                clinedata = realtime_output.strip()
                linedata += [clinedata]
    finally:
        process.kill()
        if debug:
            print("[run_sh]", "\n".join(linedata))
        return process.returncode, linedata


def f_base64(msg: str, method: str = "encode", encode_utf: str = "utf-8"):
    import base64

    if method == "encode":
        result = base64.b64encode(msg.encode(encode_utf))
    else:
        result = base64.b64decode(msg.encode(encode_utf))

    return result.decode(encode_utf)


# HELPER FUNCTION ---------------------------------------------------------------------------------------------------------------------
def f_spark_config(addition_config: dict = {}):
    spark_config = SPARK_AA_DEFAULT_CONFIG
    spark_config["hive.metastore.uris"] = spark_config.get("hive.metastore.uris", HIVE_METASTORE_URI) # thrift://ip-or-dns:9083
    spark_config["spark.master"] = spark_config.get("spark.master", "local[8]")
    spark_config["spark.local.dir"] = "/data_disk/spark_temp"

    for k, v in addition_config.items():
        spark_config[k] = v
    return spark_config


def pyspark_builder(cluster_mode: bool = False):
    from pyspark.sql import SparkSession

    spark_config = {}
    if cluster_mode:
        with open("/configs/dask-container.ip") as f:
            container_ip = f.readlines()[0].rstrip("\n")
        print("container_ip", container_ip)

        spark_config["spark.master"] = SPARK_CLUSTER_URI # spark://ip:7077

        # reroute for client mode
        spark_config["spark.driver.host"]              = INTERNAL_IP_VM  # IP internal of Deploy VM
        spark_config["spark.driver.bindAddress"]       = container_ip
        spark_config["spark.driver.port"]              = "30000"
        spark_config["spark.driver.blockManager.port"] = "30040"
        spark_config["spark.ui.port"]                  = "34040"
    else:
        spark_config["spark.master"]          = "local[8]"
        spark_config["spark.executor.memory"] = "10G"
        spark_config["spark.driver.memory"]   = "10G"

    spark_builder = SparkSession.builder
    for k, v in f_spark_config(addition_config=spark_config).items():
        spark_builder = spark_builder.config(k, v)

    return spark_builder.enableHiveSupport().getOrCreate()


def jinja_render(source: str, **context):
    from jinja2 import Environment
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    # from functools import partial

    def str_to_date(str):
        return datetime.strptime(str, "%Y-%m-%d")

    env = Environment(variable_start_string="[[", variable_end_string="]]")
    env.filters["f_date"] = macro_format_date
    env.filters["f_date_path"] = macro_format_date_path
    env.filters["strptime"] = str_to_date

    jinja_ctx = {"strptime": str_to_date,
                 "strftime": datetime.strftime,
                 "timedelta": timedelta,
                 "relativedelta": relativedelta,
                 "f_date": macro_format_date,
                 "f_date_path": macro_format_date_path,
                 }
    return env.from_string(source=source).render(**context, **jinja_ctx)


def macro_format_date_path(date) -> str:
    if type(date) == str:
        from datetime import datetime
        date = datetime.strptime(date, "%Y-%m-%d")
    return date.strftime('%Y/%m/%d')


def macro_format_date(date) -> str:
    if type(date) == str:
        return date
    return date.strftime('%Y-%m-%d')


def handle_configure(version: str=None, **configure):
    # auto tagging + schedule tagging
    schedule = configure.get("schedule", None)
    if schedule:
        schedule = schedule.split(" ")
    freq = next((i for i in range(len(schedule or [])) if schedule[i] == "*"), None)
    freq = {2: "DAILY", 3: "MONTHLY", None: "NONE"}.get(freq, "COMPLEX")

    addition_tags = [freq]
    if version:
        addition_tags += [version]
    configure["tags"] = configure.get("tags", []) + addition_tags

    # macro
    macro = configure.get("user_defined_macros", {})
    macro["f_date_path"] = macro_format_date_path
    macro["f_date"] = macro_format_date
    macro["relativedelta"] = relativedelta
    configure["user_defined_macros"] = macro

    # macro filters
    filters = configure.get("user_defined_filters", {})
    filters["f_date_path"] = macro_format_date_path
    filters["f_date"] = macro_format_date
    configure["user_defined_filters"] = filters

    # param
    params = configure.get("params", {})
    params["job_freq"] = freq
    configure["params"] = params

    return configure

# CALLABLE FUNCTION ---------------------------------------------------------------------------------------------------------------------  
def f_spark_task(pipe_msg, current_run_date, **kwargs):
    run_mode = kwargs.get("run_mode", "cluster") # cluster/local
    language = kwargs.get("language", "python")  # python/scala

    # python submit
    if language == "python":
        task_to_run = kwargs.get("callable", None)
        if task_to_run:
            with pyspark_builder(cluster_mode={"cluster": True}.get(run_mode, False)) as spark:
                return 0, task_to_run(spark, current_run_date, **kwargs)

    # SCALA submit
    elif language == "scala":
        spark_submit = SPARK_SUBMIT_PATH

        jar_path = kwargs["params"]["JAR_PATH"]
        java_class = kwargs["java_class"]
        spark_params = kwargs.get("spark_params", [])
        spark_config = kwargs.get("spark_config", SPARK_AA_DEFAULT_CONFIG)

        # render template
        spark_params = [(m[0], ",".join(m[1]) if type(m[1]) == list else m[1]) for m in spark_params]
        spark_params = [(m[0], jinja_render(m[1], run_date=current_run_date)) for m in spark_params]

        # generate cmd
        command = spark_submit
        command += " --class " + java_class
        command += " --deploy-mode cluster"
        command += " --conf " + " --conf ".join([f"{k}={v}" for k, v in spark_config.items()])
        command += " " + jar_path
        command += " " + " ".join([f"--{p[0]} {p[1]}" for p in spark_params])

        print("Start Spark:")
        result, stdout = run_sh(command)

        print(f"exit code: {result}\nstdout :\n", "\n".join(stdout))
        if result != 0 or any(line for line in stdout if "State of driver" in line and "is FAILED" in line):
            return 1, ""

        output = next(p[1] for p in spark_params if p[0] == "output")
        return 0, output


def copy_data(pipe_msg, current_run_date, **kwargs):
    source = kwargs.get("source", "")
    sink = kwargs.get("sink", "")

    source = jinja_render(source, run_date=current_run_date)
    sink = jinja_render(sink, run_date=current_run_date)

    rtcode, stdout = run_sh(f"mkdir -p {sink}")
    print("run result:", rtcode)
    print("stdout:", "\n".join(stdout))
    if rtcode != 0:
        raise Exception("Error: cannot mkdir sink path")

    rtcode, stdout = run_sh(f"cp -R {source} {sink}")
    print("run result:", rtcode)
    print("stdout:", "\n".join(stdout))
    if rtcode != 0:
        raise Exception("Error: cannot copy covid data to destination")

    return 0, sink


if __name__ == "__main__":
    pass
