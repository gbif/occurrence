package org.gbif.occurrence.download.spark;

import java.io.IOException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.sql.DropTablesWorkflow;
import org.gbif.utils.file.properties.PropertiesUtil;

public class DropTables {

  public static void main(String[] args) throws IOException {
    String downloadKey = args[0];
    WorkflowConfiguration workflowConfiguration =
        new WorkflowConfiguration(PropertiesUtil.readFromFile(args[2]));
    DwcTerm coreTerm = DwcTerm.valueOf(args[1]);

    DropTablesWorkflow.builder()
        .queryExecutorSupplier(
            () ->
                new SparkQueryExecutor(
                    SparkDownloads.createSparkSession(downloadKey, workflowConfiguration)))
        .coreDwcTerm(coreTerm)
        .downloadKey(downloadKey)
        .workflowConfiguration(workflowConfiguration)
        .build()
        .run();
  }
}
