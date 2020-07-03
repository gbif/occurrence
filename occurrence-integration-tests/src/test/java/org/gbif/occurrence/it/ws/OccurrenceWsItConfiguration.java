package org.gbif.occurrence.it.ws;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.DownloadRequestServiceImpl;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.test.mocks.DownloadCallbackServiceMock;
import org.gbif.occurrence.test.mocks.DownloadRequestServiceMock;
import org.gbif.occurrence.test.mocks.OccurrenceDownloadServiceMock;
import org.gbif.occurrence.test.servers.EsManageServer;
import org.gbif.occurrence.test.servers.HBaseServer;
import org.gbif.occurrence.ws.config.OccurrenceMethodSecurityConfiguration;
import org.gbif.occurrence.ws.config.WebMvcConfig;
import org.gbif.registry.identity.util.RegistryPasswordEncoder;
import org.gbif.ws.security.GbifUserPrincipal;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
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
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
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
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;

/**
 * SpringBoot app used for IT tests only.
 */
@TestConfiguration
@SpringBootApplication(
  exclude = {
    ElasticsearchAutoConfiguration.class,
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
   "org.gbif.ws.server.advice",
   "org.gbif.ws.server.mapper",
   "org.gbif.occurrence.search",
   "org.gbif.occurrence.ws.resources",
   "org.gbif.occurrence.persistence",
   "org.gbif.occurrence.it.ws"
 },
 excludeFilters = {
   @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, classes = {DownloadRequestServiceImpl.class})
 }
)
@PropertySource(OccurrenceWsItConfiguration.TEST_PROPERTIES)
@ActiveProfiles("test")
public class OccurrenceWsItConfiguration {

  public static final GbifUser TEST_USER = new GbifUser();

  static {
    TEST_USER.setUserName("admin");
    TEST_USER.setEmail("nothing@gbif.org");
    TEST_USER.setPasswordHash("hi");
    TEST_USER.setRoles(Collections.singleton(UserRole.USER));
  }

  public static final String TEST_PROPERTIES = "classpath:application-test.yml";

  public static final String FRAGMENT_TABLE = "fragment_table";

  public static final String RELATIONSHIPS_TABLE = "relationships_table";

  public static void main(String[] args) {
    SpringApplication.run(OccurrenceWsItConfiguration.class, args);
  }

  @Bean
  public EsManageServer esManageServer(@Value("classpath:es-settings.json") Resource settings,
                                       @Value("classpath:elasticsearch/es-occurrence-schema.json") Resource mappings) throws Exception {
    return EsManageServer.builder()
      .indexName("occurrence")
      .type("record")
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

  /**
   * Security config for ITs.
   */
  @Configuration
  public static class WebSecurityConfigurerIT extends WebSecurityConfigurerAdapter {

    /**
     * Uses an in-memory map of know users.
     */
    public static class TestAuthenticationProvider implements UserDetailsService {

      private Map<String, GbifUser> usersMap = ImmutableMap.<String, GbifUser>builder()
                                                .put(TEST_USER.getUserName(), TEST_USER)
                                                .build();

      @Override
      public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        return new GbifUserPrincipal(usersMap.get(userName));
      }
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) {
      auth.authenticationProvider(dbAuthenticationProvider());
    }

    /**
     * Simple authentication provider.
     */
    private DaoAuthenticationProvider dbAuthenticationProvider() {
      final DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider();
      authProvider.setUserDetailsService(new TestAuthenticationProvider());
      authProvider.setPasswordEncoder(passwordEncoder());
      return authProvider;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
      //Disable authentication
      http
        .httpBasic()
        .disable()
        .authorizeRequests()
        .anyRequest()
        .authenticated();

      http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
      return new RegistryPasswordEncoder();
    }
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
