package org.gbif.occurrence.it.ws;

import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.test.servers.EsManageServer;
import org.gbif.occurrence.test.servers.HBaseServer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.elasticsearch.client.RestHighLevelClient;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

/**
 * Spring configuration that provides all embedded and mock instances for IT tests.
 */
@Configuration
public class OccurrenceWsItConfiguration {

  public static final String FRAGMENT_TABLE = "fragment_table";

  public static final String RELATIONSHIPS_TABLE = "relationships_table";

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

}
