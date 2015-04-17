package org.gbif.occurrence.download.service.workflow;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.oozie.client.OozieClient;

/**
 * This class contains static fields with the expected parameters by the download workflows.
 */
public class DownloadWorkflowParameters {

  /**
   * String pattern to the shared lib of oozie workflow of specific environment.
   */
  public static final String  WORKFLOWS_PATH_FMT = "/occurrence-download-workflows-%s/lib/";

  /**
   * Constant parameters shared by all the Oozie workflows.
   */
  public static final ImmutableMap<String,String> CONSTANT_PARAMETERS = new ImmutableMap.Builder<String,String>()
                                                          .put(OozieClient.USE_SYSTEM_LIBPATH,"true")
                                                          .put("mapreduce.job.user.classpath.first", "true").build();
  /**
   * Hidden constructor.
   */
   private DownloadWorkflowParameters(){

   }

  /**
   * Parameters of the simple-csv occurrence download workflow.
   */
  public static class SimpleCsv {

    /**
     * Hidden constructor.
     */
    private SimpleCsv() {

    }
     //Hive database.
    public static final String HIVE_DB = "hive_db";

    //Download format.
    public static final String DOWNLOAD_FORMAT = "download_format";

    //Filter/Predicate.
    public static final String GBIF_FILTER = "gbif_filter";

  }
}
