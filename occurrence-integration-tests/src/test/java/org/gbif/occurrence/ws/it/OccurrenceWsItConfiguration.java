package org.gbif.occurrence.ws.it;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.DownloadRequestServiceImpl;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.test.mocks.ChallengeCodeManagerMock;
import org.gbif.occurrence.test.mocks.DownloadCallbackServiceMock;
import org.gbif.occurrence.test.mocks.DownloadRequestServiceMock;
import org.gbif.occurrence.test.mocks.GrSciCollEditorAuthorizationServiceMock;
import org.gbif.occurrence.test.mocks.OccurrenceDownloadServiceMock;
import org.gbif.occurrence.test.mocks.UserMapperMock;
import org.gbif.occurrence.test.servers.EsManageServer;
import org.gbif.occurrence.test.servers.HBaseServer;
import org.gbif.occurrence.ws.config.OccurrenceMethodSecurityConfiguration;
import org.gbif.occurrence.ws.config.WebMvcConfig;
import org.gbif.registry.identity.service.UserSuretyDelegateImpl;
import org.gbif.registry.identity.util.RegistryPasswordEncoder;
import org.gbif.registry.persistence.mapper.DatasetMapper;
import org.gbif.registry.persistence.mapper.InstallationMapper;
import org.gbif.registry.persistence.mapper.MachineTagMapper;
import org.gbif.registry.persistence.mapper.OrganizationMapper;
import org.gbif.registry.persistence.mapper.UserMapper;
import org.gbif.registry.persistence.mapper.UserRightsMapper;
import org.gbif.registry.security.EditorAuthorizationServiceImpl;
import org.gbif.registry.security.LegacyAuthorizationService;
import org.gbif.registry.security.LegacyAuthorizationServiceImpl;
import org.gbif.registry.security.config.WebSecurityConfigurer;
import org.gbif.registry.security.grscicoll.GrSciCollEditorAuthorizationFilter;
import org.gbif.registry.security.grscicoll.GrSciCollEditorAuthorizationService;
import org.gbif.registry.security.precheck.AuthPreCheckCreationRequestFilter;
import org.gbif.registry.surety.ChallengeCodeManager;
import org.gbif.registry.surety.OrganizationChallengeCodeManager;
import org.gbif.registry.surety.UserChallengeCodeManager;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.client.Connection;
import org.elasticsearch.client.RestHighLevelClient;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.ActiveProfiles;

/**
 * SpringBoot app used for IT tests only.
 */
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
   "org.gbif.registry.security",
   "org.gbif.registry.persistence",
   "org.gbif.registry.identity",
   "org.gbif.registry.surety",
   "org.gbif.occurrence.search",
   "org.gbif.occurrence.ws.resources",
   "org.gbif.occurrence.ws.identity",
   "org.gbif.occurrence.persistence"
 },
 excludeFilters = {
   @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, classes = {DownloadRequestServiceImpl.class,
                                                                      EditorAuthorizationServiceImpl.class,
                                                                      LegacyAuthorizationServiceImpl.class,
                                                                      UserSuretyDelegateImpl.class,
                                                                      UserChallengeCodeManager.class,
                                                                      OrganizationChallengeCodeManager.class,
                                                                      GrSciCollEditorAuthorizationService.class,
                                                                      WebSecurityConfigurer.class,
                                                                      AuthPreCheckCreationRequestFilter.class,
                                                                      GrSciCollEditorAuthorizationFilter.class})
 }
)
@PropertySource(OccurrenceWsItConfiguration.TEST_PROPERTIES)
@ActiveProfiles("test")
public class OccurrenceWsItConfiguration {

  public static final GbifUser TEST_USER = new GbifUser();
  public static final String TEST_USER_PASSWORD = "hi";

  public static final RegistryPasswordEncoder PASSWORD_ENCODER = new RegistryPasswordEncoder();

  static {
    TEST_USER.setUserName("admin");
    TEST_USER.setEmail("nothing@gbif.org");
    TEST_USER.setPasswordHash(PASSWORD_ENCODER.encode(TEST_USER_PASSWORD));
    TEST_USER.setRoles(Collections.singleton(UserRole.USER));
  }

  public static final String TEST_PROPERTIES = "classpath:application-test.yml";

  public static final String FRAGMENT_TABLE = "fragment_table";

  public static final String RELATIONSHIPS_TABLE = "relationships_table";

  public static void main(String[] args) {
    SpringApplication.run(OccurrenceWsItConfiguration.class, args);
  }

  @Bean
  public EsManageServer esManageServer(@Value("classpath:elasticsearch/es-settings.json") Resource settings,
                                       @Value("classpath:elasticsearch/es-occurrence-schema.json") Resource mappings) throws Exception {
    return EsManageServer.builder()
      .indexName("occurrence")
      .keyField("gbifId")
      .settingsFile(settings)
      .mappingFile(mappings).build();
  }

  /**
   * EsConfig made from an EsManagerServer.
   */
  @ConfigurationProperties(prefix = "occurrence.search.es")
  @Bean
  public EsConfig esConfig(EsManageServer esManageServer) {
    EsConfig esConfig = new EsConfig();
    esConfig.setHosts(new String[]{esManageServer.getServerAddress()});
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

  /**
   * Mock name matching service.
   */
  @Bean
  public NameUsageMatchingService nameUsageMatchingService() {
    return Mockito.mock(NameUsageMatchingService.class);
  }

  /**
   * Mock service to interact with the Registry.
   */
  @Bean
  @ConditionalOnProperty(name = "api.url", matchIfMissing = true)
  public OccurrenceDownloadService occurrenceDownloadService() {
    return new OccurrenceDownloadServiceMock();
  }

  /**
   * Creates a mock Callback service to abstract Oozie handlers.
   */
  @Bean
  public CallbackService downloadCallbackService(OccurrenceDownloadService occurrenceDownloadService) {
    return new DownloadCallbackServiceMock(occurrenceDownloadService);
  }

  /**
   * Creates a DownloadRequestService using the available mock instances.
   */
  @Bean
  public DownloadRequestService downloadRequestService(OccurrenceDownloadService occurrenceDownloadService,
                                                       CallbackService callbackService,
                                                       ResourceLoader resourceLoader) {
    return new DownloadRequestServiceMock(occurrenceDownloadService, callbackService, resourceLoader);
  }

  /**
   * Managed Spring bean that contains an embedded HBase mini-cluster.
   */
  @Bean
  public HBaseServer hBaseServer() {
    return new HBaseServer();
  }

  /**
   * Gets a connection from the embedded HBase mini-cluster.
   */
  @Bean
  public Connection hBaseConnection(HBaseServer hBaseServer) throws IOException {
    return hBaseServer.getConnection();
  }

  /**
   * HBase configuration made from HBase mini-cluster.
   */
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
  public UserMapper userMapperMock() {
    UserMapper userMapper = new UserMapperMock();
    userMapper.create(TEST_USER);
    return userMapper;
  }

  @Bean
  public ChallengeCodeManager<Integer> challengeCodeManagerMock() {
    return new ChallengeCodeManagerMock();
  }


  @Bean
  public EditorAuthorizationServiceImpl editorAuthorizationServiceSutb() {
    return new EditorAuthorizationServiceImpl(Mockito.mock(OrganizationMapper.class),
                                              Mockito.mock(DatasetMapper.class),
                                              Mockito.mock(InstallationMapper.class),
                                              Mockito.mock(UserRightsMapper.class),
                                              Mockito.mock(MachineTagMapper.class));
  }

  @Bean
  public LegacyAuthorizationService legacyAuthorizationService() {
    return new LegacyAuthorizationServiceImpl(Mockito.mock(OrganizationMapper.class),
                                       Mockito.mock(DatasetMapper.class),
                                       Mockito.mock(InstallationMapper.class));
  }

  @Bean
  public GrSciCollEditorAuthorizationService grSciCollEditorAuthorization() {
    return new GrSciCollEditorAuthorizationServiceMock();
  }

  /**
   * Empty config class to include the config made by WebMvcConfig.
   */
  @Configuration
  public static class WebMvcConfigIT extends WebMvcConfig{}

  /**
   * Empty config class to include the config made by OccurrenceMethodSecurityConfiguration.
   */
  @Configuration
  public static class OccurrenceMethodSecurityConfigurationIT extends OccurrenceMethodSecurityConfiguration{}

}
