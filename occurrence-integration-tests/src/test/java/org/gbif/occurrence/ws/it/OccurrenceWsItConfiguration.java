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
package org.gbif.occurrence.ws.it;

import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.DownloadRequestServiceImpl;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.test.mocks.*;
import org.gbif.occurrence.test.servers.EsManageServer;
import org.gbif.occurrence.test.servers.HBaseServer;
import org.gbif.occurrence.ws.config.WebMvcConfig;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.LoggedUser;
import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
import org.gbif.ws.security.*;
import org.gbif.ws.server.filter.AppIdentityFilter;
import org.gbif.ws.server.filter.IdentityFilter;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.client.Connection;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Disabled;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.elastic.ElasticMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.zookeeper.ZookeeperAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.ActiveProfiles;

/** SpringBoot app used for IT tests only. */
@TestConfiguration
@SpringBootApplication(
    exclude = {
      ElasticSearchRestHealthContributorAutoConfiguration.class,
      RabbitAutoConfiguration.class,
      ElasticMetricsExportAutoConfiguration.class,
      ZookeeperAutoConfiguration.class
    })
@EnableConfigurationProperties
@EnableFeignClients
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.ws.security",
      "org.gbif.ws.remoteauth",
      "org.gbif.occurrence.search",
      "org.gbif.occurrence.ws.resources",
      "org.gbif.occurrence.persistence"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            DownloadRequestServiceImpl.class,
            AppKeySigningService.class,
            FileSystemKeyStore.class,
            IdentityFilter.class,
            AppIdentityFilter.class,
            GbifAuthenticationManagerImpl.class,
            GbifAuthServiceImpl.class
          })
    })
@PropertySource(OccurrenceWsItConfiguration.TEST_PROPERTIES)
@ActiveProfiles("test")
@Disabled
public class OccurrenceWsItConfiguration {

  public static final LoggedUser TEST_USER =
      LoggedUser.builder()
          .userName("admin")
          .email("nothing@gbif.org")
          .roles(Collections.singleton(UserRole.USER.name()))
          .build();

  public static final String TEST_PROPERTIES = "classpath:application-test.yml";

  public static final String FRAGMENT_TABLE = "fragment_table";

  public static final String RELATIONSHIPS_TABLE = "relationships_table";

  public static void main(String[] args) {
    SpringApplication.run(OccurrenceWsItConfiguration.class, args);
  }

  @Bean
  public EsManageServer esManageServer(
      @Value("classpath:elasticsearch/es-settings.json") Resource settings,
      @Value("classpath:elasticsearch/es-occurrence-schema.json") Resource mappings)
      throws Exception {
    return EsManageServer.builder()
        .indexName("occurrence")
        .keyField("gbifId")
        .settingsFile(settings)
        .mappingFile(mappings)
        .build();
  }

  /** EsConfig made from an EsManagerServer. */
  @ConfigurationProperties(prefix = "occurrence.search.es")
  @Bean
  public EsConfig esConfig(EsManageServer esManageServer) {
    EsConfig esConfig = new EsConfig();
    esConfig.setHosts(new String[] {esManageServer.getServerAddress()});
    esConfig.setIndex(esManageServer.getIndexName());
    return esConfig;
  }

  /**
   * Creates an instance of an Elasticsearch RestHighLevelClient from the embedded EsManagerServer.
   */
  @Bean
  public RestHighLevelClient restHighLevelClient(EsManageServer esManageServer) {
    return esManageServer.getRestClient();
  }

  /** Mock name matching service. */
  @Bean
  public NameUsageMatchingService nameUsageMatchingService() {
    return Mockito.mock(NameUsageMatchingService.class);
  }

  /** Mock service to interact with the Registry. */
  @Bean
  @ConditionalOnProperty(name = "api.url", matchIfMissing = true)
  public OccurrenceDownloadService occurrenceDownloadService() {
    return new OccurrenceDownloadServiceMock();
  }

  /** Creates a mock Callback service to abstract Oozie handlers. */
  @Bean
  public CallbackService downloadCallbackService(
      OccurrenceDownloadService occurrenceDownloadService) {
    return new DownloadCallbackServiceMock(occurrenceDownloadService);
  }

  /** Creates a DownloadRequestService using the available mock instances. */
  @Bean
  public DownloadRequestService downloadRequestService(
      OccurrenceDownloadService occurrenceDownloadService,
      CallbackService callbackService,
      ResourceLoader resourceLoader) {
    return new DownloadRequestServiceMock(
        occurrenceDownloadService, callbackService, resourceLoader);
  }

  /** Managed Spring bean that contains an embedded HBase mini-cluster. */
  @Bean
  public HBaseServer hBaseServer() {
    return new HBaseServer();
  }

  /** Gets a connection from the embedded HBase mini-cluster. */
  @Bean
  public Connection hBaseConnection(HBaseServer hBaseServer) throws IOException {
    return hBaseServer.getConnection();
  }

  /** HBase configuration made from HBase mini-cluster. */
  @Bean
  public OccHBaseConfiguration occHBaseConfiguration(HBaseServer hBaseServer) {
    OccHBaseConfiguration occHBaseConfiguration = new OccHBaseConfiguration();
    occHBaseConfiguration.setZkConnectionString(hBaseServer.getZKClusterKey());
    occHBaseConfiguration.setFragmenterTable(FRAGMENT_TABLE);
    occHBaseConfiguration.setRelationshipTable(RELATIONSHIPS_TABLE);
    occHBaseConfiguration.setRelationshipSalt(1);
    occHBaseConfiguration.setFragmenterSalt(1);
    return occHBaseConfiguration;
  }

  @Bean
  public RemoteAuthClient remoteAuthClient() {
    return RemoteAuthClientMock.builder().testUser(TEST_USER).build();
  }

  @Bean
  public IdentityServiceClient identityAccessServiceClient() {
    return IdentityServiceClientMock.builder().testUser(TEST_USER).build();
  }

  /** Empty config class to include the config made by WebMvcConfig. */
  @Configuration
  public static class WebMvcConfigIT extends WebMvcConfig {}

  /** Empty config class to include the config made by OccurrenceMethodSecurityConfiguration. */
  @Configuration
  public static class OccurrenceMethodSecurityConfigurationIT
      extends RoleMethodSecurityConfiguration {}

  @Configuration
  public class SecurityConfiguration extends RemoteAuthWebSecurityConfigurer {

    public SecurityConfiguration(ApplicationContext context, RemoteAuthClient remoteAuthClient) {
      super(context, remoteAuthClient);
    }
  }
}
