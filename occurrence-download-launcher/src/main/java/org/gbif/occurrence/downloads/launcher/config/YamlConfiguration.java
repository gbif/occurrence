package org.gbif.occurrence.downloads.launcher.config;

import com.google.common.collect.ImmutableMap;
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
  @Qualifier("oozie.default_properties")
  public Map<String, String> providesDefaultParameters(
      @Value("${occurrence.download.environment}") String environment,
      @Value("${occurrence.download.ws.url}") String wsUrl,
      @Value("${occurrence.download.hdfs.namenode}") String nameNode,
      @Value("${occurrence.download.username}") String userName,
      @Value("${occurrence.download.type}") DownloadType type) {
    return new ImmutableMap.Builder<String, String>()
        .put(
            OozieClient.LIBPATH,
            String.format(
                DownloadWorkflowParameters.WORKFLOWS_LIB_PATH_FMT, "occurrence", environment))
        .put(
            OozieClient.APP_PATH,
            nameNode
                + String.format(
                    DownloadWorkflowParameters.DOWNLOAD_WORKFLOW_PATH_FMT,
                    "occurrence",
                    environment))
        .put(
            OozieClient.WORKFLOW_NOTIFICATION_URL,
            DownloadUtils.concatUrlPaths(
                wsUrl,
                type.name().toLowerCase()
                    + "/download/request/callback?job_id=$jobId&status=$status"))
        .put(OozieClient.USER_NAME, userName)
        .put(DownloadWorkflowParameters.CORE_TERM_NAME, type.getCoreTerm().name())
        .put(DownloadWorkflowParameters.TABLE_NAME, type.getCoreTerm().name().toLowerCase())
        .putAll(DownloadWorkflowParameters.CONSTANT_PARAMETERS)
        .build();
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
