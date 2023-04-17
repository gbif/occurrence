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
