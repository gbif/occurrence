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
package org.gbif.occurrence.download.spark;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.sql.DownloadWorkflow;
import org.gbif.occurrence.spark.udf.UDFS;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkDownloads {


  public static void main(String[] args) {
    String downloadKey = args[0];
    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();
    DownloadWorkflow.builder()
      .downloadKey(downloadKey)
      .coreDwcTerm(DwcTerm.valueOf(args[1]))
      .workflowConfiguration(workflowConfiguration)
      .queryExecutorSupplier(() -> new SparkQueryExecutor(createSparkSession(args[0], workflowConfiguration)))
      .build()
      .run();
  }

  public static SparkSession createSparkSession(String downloadKey, WorkflowConfiguration workflowConfiguration) {
    SparkConf sparkConf = new SparkConf()
      .set("spark.sql.warehouse.dir", workflowConfiguration.getHiveWarehouseDir());
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName("Download job " + downloadKey)
      .config(sparkConf)
      .enableHiveSupport();
    SparkSession session = sparkBuilder.getOrCreate();

    UDFS.registerUdfs(session);

    return session;
  }

}
