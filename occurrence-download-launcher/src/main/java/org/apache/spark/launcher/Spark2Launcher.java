/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.launcher;

/**
 * This is a workaround that enables the ability to run 'spark2-submit' while avoiding any 'SPARK_HOME' issues
 */
public class Spark2Launcher extends SparkLauncher {

  /**
   * To prevent SparkLauncher from using incorrect variables for Spark, it's necessary to remove 'SPARK_HOME' from the
   * builder map since it checks all environment variables before calling 'findSparkSubmit()'
   * <p>
   * Since SparkLauncher can only run 'spark-submit', we can override it to use 'spark2-submit'
   */
  @Override
  String findSparkSubmit() {
    this.builder.childEnv.remove("SPARK_HOME");
    return "spark2-submit";
  }

}
