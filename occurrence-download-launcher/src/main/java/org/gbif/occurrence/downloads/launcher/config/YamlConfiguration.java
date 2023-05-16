package org.gbif.occurrence.downloads.launcher.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.oozie.client.OozieClient;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class YamlConfiguration {

  @Bean
  @Qualifier("oozie.properties")
  public Map<String, String> providesDefaultParameters(
      @Value("${occurrence.download.environment}") String environment,
      @Value("${occurrence.download.ws.url}") String wsUrl,
      @Value("${occurrence.download.hdfs.namenode}") String nameNode,
      @Value("${occurrence.download.username}") String userName,
      @Value("${occurrence.download.type}") DownloadType type) {
    Map<String, String> config = new HashMap<>();

    // Dynamic values
    config.put(
        OozieClient.LIBPATH,
        String.format(
            DownloadWorkflowParameters.WORKFLOWS_LIB_PATH_FMT, "occurrence", environment));
    config.put(
        OozieClient.APP_PATH,
        nameNode
            + String.format(
                DownloadWorkflowParameters.DOWNLOAD_WORKFLOW_PATH_FMT, "occurrence", environment));
    config.put(
        OozieClient.WORKFLOW_NOTIFICATION_URL,
        DownloadUtils.concatUrlPaths(
            wsUrl,
            type.name().toLowerCase() + "/download/request/callback?job_id=$jobId&status=$status"));
    config.put(OozieClient.USER_NAME, userName);
    config.put(DownloadWorkflowParameters.CORE_TERM_NAME, type.getCoreTerm().name());
    config.put(DownloadWorkflowParameters.TABLE_NAME, type.getCoreTerm().name().toLowerCase());

    // Fixed values
    config.put(OozieClient.USE_SYSTEM_LIBPATH, "true");
    config.put("mapreduce.job.user.classpath.first", "true");

    return Collections.unmodifiableMap(config);
  }

  @ConfigurationProperties(prefix = "downloads")
  @Bean
  public DownloadServiceConfiguration downloadServiceConfiguration() {
    return new DownloadServiceConfiguration();
  }

  @ConfigurationProperties(prefix = "spark")
  @Bean
  public SparkConfiguration sparkConfiguration() {
    return new SparkConfiguration();
  }

  @ConfigurationProperties(prefix = "registry")
  @Bean
  public RegistryConfiguration registryConfiguration() {
    return new RegistryConfiguration();
  }
}
