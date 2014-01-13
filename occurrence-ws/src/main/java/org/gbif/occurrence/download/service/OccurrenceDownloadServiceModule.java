package org.gbif.occurrence.download.service;

import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.occurrence.common.download.DownloadUtils;
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
  private static final String JOB_TRACKER = "jobtracker";
  private static final String NAME_NODE = "namenode";
  private final String regUrl;

  public OccurrenceDownloadServiceModule(Properties properties) {
    super(PREFIX, properties);
    regUrl = properties.getProperty("registry.ws.url");
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
  Map<String, String> providesOozieDefaultProperties(@Named(NAME_NODE) String nameNode,
    @Named(JOB_TRACKER) String jobTracker, @Named("oozie.workflow.path") String workflowPath,
    @Named("hive.hdfs.out") String hdfsOutput, @Named("ws.url") String wsUrl,
    @Named("hive.table") String occurrenceTable, @Named("oozie.mount") String oozieDownloadMount) {

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.put(OozieClient.APP_PATH, workflowPath)
      .put(OozieClient.USER_NAME, Constants.OOZIE_USER)
      .put(OozieClient.WORKFLOW_NOTIFICATION_URL,
        DownloadUtils.concatUrlPaths(wsUrl, "occurrence/download/request/callback?job_id=$jobId&status=$status"))
      .put(JOB_TRACKER, jobTracker)
      .put(NAME_NODE, nameNode)
      .put("hdfs_hive_path", hdfsOutput)
      .put("occurrence_table", occurrenceTable)
      .put("download_mount", oozieDownloadMount)
      .put("mapreduce.task.classpath.user.precedence", "true")
      .put("registry_ws", regUrl);
    // we dont have a specific downloadId yet, submit a placeholder
    String downloadLinkTemplate = DownloadUtils.concatUrlPaths(wsUrl,
      "occurrence/download/" + DownloadUtils.DOWNLOAD_ID_PLACEHOLDER + ".zip");
    builder.put("download_link", downloadLinkTemplate);

    return builder.build();
  }
}
