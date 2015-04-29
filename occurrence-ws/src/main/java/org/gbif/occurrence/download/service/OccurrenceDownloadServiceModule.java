package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.service.workflow.DownloadWorkflowParameters;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Map;
import java.util.Properties;
import javax.mail.Session;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.oozie.client.OozieClient;

public class OccurrenceDownloadServiceModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.download.";

  public OccurrenceDownloadServiceModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    bind(DownloadEmailUtils.class);
    bind(DownloadRequestService.class).to(DownloadRequestServiceImpl.class);
    expose(DownloadRequestService.class);

    bind(CallbackService.class).to(DownloadRequestServiceImpl.class);
    expose(CallbackService.class);
  }

  @Provides
  @Singleton
  Session providesMailSession(@Named("mail.smtp") String smtpServer,
    @Named("mail.from") String fromAddress) {
    Properties props = new Properties();
    props.setProperty("mail.smtp.host", smtpServer);
    props.setProperty("mail.from", fromAddress);
    return Session.getInstance(props, null);
  }

  @Provides
  @Singleton
  OozieClient providesOozieClient(@Named("oozie.url") String url) {
    return new OozieClient(url);
  }

  @Provides
  @Singleton
  @Named("oozie.default_properties")
  Map<String, String> providesOozieDefaultProperties(@Named("ws.url") String wsUrl,
                                                     @Named("oozie.workflow.path") String workflowPath, @Named("hive.hdfs.out") String hdfsOutput) {

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.put(OozieClient.APP_PATH, workflowPath)
      .put(OozieClient.USER_NAME, Constants.OOZIE_USER)
      .put("hdfs_hive_path", hdfsOutput)
      .put(OozieClient.WORKFLOW_NOTIFICATION_URL,
           DownloadUtils.concatUrlPaths(wsUrl, "occurrence/download/request/callback?job_id=$jobId&status=$status"))
      .put(OozieClient.USE_SYSTEM_LIBPATH,"true")
      .put("oozie.action.sharelib.for.hive", "hive");
    // we dont have a specific downloadId yet, submit a placeholder
    String downloadLinkTemplate = DownloadUtils.concatUrlPaths(wsUrl,
                                                               "occurrence/download/" + DownloadUtils.DOWNLOAD_ID_PLACEHOLDER + ".zip");
    builder.put("download_link", downloadLinkTemplate);

    return builder.build();
  }

  @Provides
  @Singleton
  @Named("oozie.default_simplecsv_properties")
  Map<String,String> providesSimpleCsvDefaultParameters(@Named("hive_db") String hiveDB,
                                                        @Named("environment") String environment,
                                                        @Named("ws.url") String wsUrl,
                                                        @Named("hdfs.namenode") String nameNode){
    return new ImmutableMap.Builder<String, String>().put(DownloadWorkflowParameters.SimpleCsv.HIVE_DB, hiveDB)
                                              .put(DownloadWorkflowParameters.SimpleCsv.DOWNLOAD_FORMAT, DownloadFormat.SIMPLE_CSV.name())
                                              .put(OozieClient.LIBPATH,String.format(DownloadWorkflowParameters.WORKFLOWS_LIB_PATH_FMT,environment))
                                               .put(OozieClient.APP_PATH, nameNode + String.format(DownloadWorkflowParameters.SimpleCsv.WORKFLOW_PATH_FMT,environment))
                                              .put(OozieClient.WORKFLOW_NOTIFICATION_URL, DownloadUtils.concatUrlPaths(wsUrl, "occurrence/download/request/callback?job_id=$jobId&status=$status"))
                                              .put(OozieClient.USER_NAME, Constants.OOZIE_USER)
                                              .putAll(DownloadWorkflowParameters.CONSTANT_PARAMETERS).build();
  }
}
