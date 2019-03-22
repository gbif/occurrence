package org.gbif.occurrence.download.service.workflow;

import com.google.common.collect.ImmutableMap;
import org.apache.oozie.client.OozieClient;

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

  public static final String SQL_EXPORT_TEMPLATE = "sql_export_template";

  /**
   * Private default constructor.
   */
  private DownloadWorkflowParameters() {
    //hidden constructor
  }

}
