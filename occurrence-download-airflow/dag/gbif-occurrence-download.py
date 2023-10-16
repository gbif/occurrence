#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow import DAG
from airflow.models.param import Param
from airflow.operators import bash_operator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='occurrence_download',
    schedule_interval=None,
    start_date=datetime.now(),
    params={
      "jar_file": Param("", type="string"),
      "download_key": Param("", type="string"),
      "config_file": Param("", type="string")
    }
) as dag:

    t_main = bash_operator.BashOperator(
            task_id='run_download',
            dag=dag,
            bash_command='java --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar {jar_file} {config_file} {download_key}'.format(jar_file='{{ params.jar_file }}', download_key='{{ params.download_key }}', config_file='{{ params.config_file }}')
        )

    t_main
