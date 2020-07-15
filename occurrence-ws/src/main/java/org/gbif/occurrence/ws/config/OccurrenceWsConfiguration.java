package org.gbif.occurrence.ws.config;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.service.workflow.DownloadWorkflowParameters;
import org.gbif.occurrence.persistence.configuration.OccurrencePersistenceConfiguration;
import org.gbif.occurrence.search.configuration.OccurrenceSearchConfiguration;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientFactory;

import org.gbif.occurrence.query.TitleLookupService;
import org.gbif.occurrence.query.TitleLookupServiceFactory;

import java.util.Map;
import java.util.Properties;

import javax.mail.Session;

import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.oozie.client.OozieClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class OccurrenceWsConfiguration {

  @Bean
  @Primary
  @ConfigurationProperties("registry.datasource")
  public DataSourceProperties registryDataSourceProperties() {
    return new DataSourceProperties();
  }

  @Bean
  @Primary
  @ConfigurationProperties("registry.datasource.hikari")
  public HikariDataSource registryDataSource() {
    return registryDataSourceProperties()
      .initializeDataSourceBuilder()
      .type(HikariDataSource.class)
      .build();
  }

  @Bean
  public OozieClient providesOozieClient(@Value("${occurrence.download.oozie.url}") String url) {
    return new OozieClient(url);
  }

  @Bean
  public Session providesMailSession(@Value("${occurrence.download.mail.smtp}") String smtpServer, @Value("${occurrence.download.mail.from}") String fromAddress) {
    Properties props = new Properties();
    props.setProperty("mail.smtp.host", smtpServer);
    props.setProperty("mail.from", fromAddress);
    return Session.getInstance(props, null);
  }


  @Bean
  @Qualifier("oozie.default_properties")
  public Map<String,String> providesDefaultParameters(@Value("${occurrence.download.environment}") String environment,
                                               @Value("${occurrence.download.ws.url}") String wsUrl,
                                               @Value("${occurrence.download.hdfs.namenode}") String nameNode,
                                               @Value("${occurrence.download.username}") String userName) {
    return new ImmutableMap.Builder<String, String>()
      .put(OozieClient.LIBPATH, String.format(DownloadWorkflowParameters.WORKFLOWS_LIB_PATH_FMT, environment))
      .put(OozieClient.APP_PATH, nameNode + String.format(DownloadWorkflowParameters.DOWNLOAD_WORKFLOW_PATH_FMT,
                                                          environment))
      .put(OozieClient.WORKFLOW_NOTIFICATION_URL,
           DownloadUtils.concatUrlPaths(wsUrl, "occurrence/download/request/callback?job_id=$jobId&status=$status"))
      .put(OozieClient.USER_NAME, userName)
      .putAll(DownloadWorkflowParameters.CONSTANT_PARAMETERS).build();
  }

  @Bean
  public TitleLookupService titleLookupService(@Value("${api.url}") String apiUrl) {
    return TitleLookupServiceFactory.getInstance(apiUrl);
  }

  @Bean
  public OccurrenceDownloadService occurrenceDownloadService(@Value("${api.url}") String apiUrl,
                                                             @Value("${occurrence.download.ws.username}") String downloadUsername,
                                                             @Value("${occurrence.download.ws.password}") String downloadUserPassword) {
    ClientFactory clientFactory = new ClientFactory(downloadUsername, downloadUserPassword, apiUrl);
    return clientFactory.newInstance(OccurrenceDownloadClient.class);
  }

  @Configuration
  public static class OccurrenceSearchConfigurationWs extends OccurrenceSearchConfiguration {

  }

  @Configuration
  public static class OccurrencePersistenceConfigurationWs extends OccurrencePersistenceConfiguration {

  }
}
