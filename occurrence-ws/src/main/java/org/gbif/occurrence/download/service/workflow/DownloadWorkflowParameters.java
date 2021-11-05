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
package org.gbif.occurrence.download.service.workflow;

import org.apache.oozie.client.OozieClient;

import com.google.common.collect.ImmutableMap;

/**
 * This class contains static fields with the expected parameters by the download workflows.
 */
public class DownloadWorkflowParameters {

  /**
   * String pattern to the shared lib of oozie workflow of specific environment.
   */
  public static final String  WORKFLOWS_PATH_FMT = "/occurrence-download-workflows-%s/";

  public static final String  DOWNLOAD_WORKFLOW_PATH_FMT = WORKFLOWS_PATH_FMT + "download-workflow";

  public static final String  WORKFLOWS_LIB_PATH_FMT = WORKFLOWS_PATH_FMT + "lib/";
  
  /**
   * Constant parameters shared by all the Oozie workflows.
   */
  public static final ImmutableMap<String,String> CONSTANT_PARAMETERS = new ImmutableMap.Builder<String, String>()
                                                          .put(OozieClient.USE_SYSTEM_LIBPATH, "true")
                                                          .put("mapreduce.job.user.classpath.first", "true").build();
  //Download format.
  public static final String DOWNLOAD_FORMAT = "download_format";

  //Filter/Predicate/SQL.
  public static final String GBIF_FILTER = "gbif_filter";
  

  public static final String SQL_HEADER = "sql_header";
  
  public static final String SQL_WHERE = "sql_where";

  /**
   * Private default constructor.
   */
  private DownloadWorkflowParameters() {
    //hidden constructor
  }

}
