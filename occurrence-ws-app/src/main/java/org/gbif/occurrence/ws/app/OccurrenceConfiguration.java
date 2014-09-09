package org.gbif.occurrence.ws.app;

import java.lang.reflect.Field;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.Resources;
import io.dropwizard.Configuration;
import org.yaml.snakeyaml.Yaml;


public class OccurrenceConfiguration extends Configuration {

  @PropertiesKey("registry.ws.url")
  private String registryWsUrl;

  @PropertiesKey("checklistbank.ws.url")
  private String checklistbankWsUrl;

  @PropertiesKey("checklistbank.match.ws.url")
  private String checklistbankMatchWsUrl;

  // drupal db for user service
  @PropertiesKey("drupal.db.JDBC.driver")
  private String drupalDbJdbcDriver;

  @PropertiesKey("drupal.db.JDBC.url")
  private String drupalDbJdbcUrl;

  @PropertiesKey("drupal.db.JDBC.username")
  private String drupalDbJdbcUsername;

  @PropertiesKey("drupal.db.JDBC.password")
  private String drupalDbJdbcPassword;

  // file with all application keys & secrets
  @PropertiesKey("appkeys.file")
  private String appkeysFile;

  @PropertiesKey("occurrence.db.table_name")
  private String occurrenceDbTableName;// =${occurrence.env_prefix}_occurrence

  @PropertiesKey("occurrence.db.featured_table_name")
  private String occurrenceDbFeaturedTableName; // ${occurrence.env_prefix}_featured_occurrence

  @PropertiesKey("occurrence.db.counter_table_name")
  private String occurrenceDbCounterTableName; // ${occurrence.env_prefix}_occurrence_counter

  @PropertiesKey("occurrence.db.id_lookup_table_name")
  private String occurrenceDbIdLookupTableName; // ${occurrence.env_prefix}_occurrence_lookup

  @PropertiesKey("occurrence.db.max_connection_pool")
  private String occurrenceDbMaxConnectionPool; // ${hbase.maxConnectionPool}

  @PropertiesKey("occurrence.db.zookeeper.connection_string")
  private String occurrenceDbZookeeperConnectionString; // ${zookeeper.quorum}

  @PropertiesKey("occurrence.search.solr.server")
  private String searchSolrServer; // ${occurrence.search.solr.server}

  @PropertiesKey("occurrence.search.solr.server.type")
  private String searchSolrServerType; // HTTP

  @PropertiesKey("occurrence.search.solr.server.httpLbservers")
  private String searchSolrServerHttpLbservers;

  @PropertiesKey("occurrence.search.solr.embedded")
  private Boolean searchSolrEmbedded = false;

  @PropertiesKey("occurrence.search.solr.request_handler")
  private String searchSolrRequestHandler;

  @PropertiesKey("occurrence.search.solr.zk_host")
  private String searchSolrZkHost;

  @PropertiesKey("occurrence.search.solr.collection")
  private String searchSolrCollection;

  @PropertiesKey("occurrence.download.ws.username")
  private String downloadWsUsername;// ${occurrence.download.ws.username}

  @PropertiesKey("occurrence.download.ws.password")
  private String downloadWsPassword;// ${occurrence.download.ws.password}

  @PropertiesKey("occurrence.download.ws.url")
  private String downloadWsUrl;// ${occurrence.download.ws.url}

  @PropertiesKey("occurrence.download.datause.url")
  private String downloadDatauseUrl;// ${occurrence.download.datause.url}

  @PropertiesKey("occurrence.download.ws.mount")
  private String downloadWsMount;// ${occurrence.download.ws.mount}

  @PropertiesKey("occurrence.download.oozie.url")
  private String downloadOozieUrl;// ${oozie.url}

  @PropertiesKey("occurrence.download.oozie.workflow.path")
  private String downloadOozieWorkflowPath;// ${hdfs.namenode}/occurrence-download/${occurrence.environment}

  @PropertiesKey("occurrence.download.hive.hdfs.out")
  private String downloadHiveHdfsOut;// ${occurrence.download.hive.hdfs.out}

  @PropertiesKey("occurrence.download.mail.smtp")
  private String downloadMailSmtp;// ${mail.smtp}

  @PropertiesKey("occurrence.download.mail.from")
  private String downloadMailFrom;// ${occurrence.download.mail.from}

  @PropertiesKey("occurrence.download.mail.bcc")
  private String downloadMailBcc;// ${occurrence.download.mail.bcc}


  @JsonProperty
  public String getRegistryWsUrl() {
    return registryWsUrl;
  }


  public void setRegistryWsUrl(String registryWsUrl) {
    this.registryWsUrl = registryWsUrl;
  }


  @JsonProperty
  public String getChecklistbankWsUrl() {
    return checklistbankWsUrl;
  }


  public void setChecklistbankWsUrl(String checklistbankWsUrl) {
    this.checklistbankWsUrl = checklistbankWsUrl;
  }


  @JsonProperty
  public String getChecklistbankMatchWsUrl() {
    return checklistbankMatchWsUrl;
  }


  public void setChecklistbankMatchWsUrl(String checklistbankMatchWsUrl) {
    this.checklistbankMatchWsUrl = checklistbankMatchWsUrl;
  }


  @JsonProperty
  public String getDrupalDbJdbcDriver() {
    return drupalDbJdbcDriver;
  }


  public void setDrupalDbJdbcDriver(String drupalDbJdbcDriver) {
    this.drupalDbJdbcDriver = drupalDbJdbcDriver;
  }

  @JsonProperty
  public String getDrupalDbJdbcUrl() {
    return drupalDbJdbcUrl;
  }

  public void setDrupalDbJdbcUrl(String drupalDbJdbcUrl) {
    this.drupalDbJdbcUrl = drupalDbJdbcUrl;
  }


  @JsonProperty
  public String getDrupalDbJdbcUsername() {
    return drupalDbJdbcUsername;
  }


  public void setDrupalDbJdbcUsername(String drupalDbJdbcUsername) {
    this.drupalDbJdbcUsername = drupalDbJdbcUsername;
  }

  @JsonProperty
  public String getDrupalDbJdbcPassword() {
    return drupalDbJdbcPassword;
  }


  public void setDrupalDbJdbcPassword(String drupalDbJdbcPassword) {
    this.drupalDbJdbcPassword = drupalDbJdbcPassword;
  }

  @JsonProperty
  public String getAppkeysFile() {
    return appkeysFile;
  }


  public void setAppkeysFile(String appkeysFile) {
    this.appkeysFile = appkeysFile;
  }

  @JsonProperty
  public String getOccurrenceDbTableName() {
    return occurrenceDbTableName;
  }


  public void setOccurrenceDbTableName(String occurrenceDbTable_name) {
    this.occurrenceDbTableName = occurrenceDbTable_name;
  }

  @JsonProperty
  public String getOccurrenceDbFeaturedTableName() {
    return occurrenceDbFeaturedTableName;
  }


  public void setOccurrenceDbFeaturedTableName(String occurrenceDbFeaturedTableName) {
    this.occurrenceDbFeaturedTableName = occurrenceDbFeaturedTableName;
  }

  @JsonProperty
  public String getOccurrenceDbCounterTableName() {
    return occurrenceDbCounterTableName;
  }


  public void setOccurrenceDbCounterTableName(String occurrenceDbCounterTableName) {
    this.occurrenceDbCounterTableName = occurrenceDbCounterTableName;
  }

  @JsonProperty
  public String getOccurrenceDbIdLookupTableName() {
    return occurrenceDbIdLookupTableName;
  }


  public void setOccurrenceDbIdLookupTableName(String occurrenceDbIdLookupTableName) {
    this.occurrenceDbIdLookupTableName = occurrenceDbIdLookupTableName;
  }

  @JsonProperty
  public String getOccurrenceDbMaxConnectionPool() {
    return occurrenceDbMaxConnectionPool;
  }


  public void setOccurrenceDbMaxConnectionPool(String occurrenceDbMaxConnectionPool) {
    this.occurrenceDbMaxConnectionPool = occurrenceDbMaxConnectionPool;
  }

  @JsonProperty
  public String getOccurrenceDbZookeeperConnectionString() {
    return occurrenceDbZookeeperConnectionString;
  }


  public void setOccurrenceDbZookeeperConnectionString(String occurrenceDbZookeeperConnectionString) {
    this.occurrenceDbZookeeperConnectionString = occurrenceDbZookeeperConnectionString;
  }

  @JsonProperty
  public String getSearchSolrServer() {
    return searchSolrServer;
  }


  public void setSearchSolrServer(String searchSolrServer) {
    this.searchSolrServer = searchSolrServer;
  }

  @JsonProperty
  public String getSearchSolrServerType() {
    return searchSolrServerType;
  }


  public void setSearchSolrServerType(String searchSolrServerType) {
    this.searchSolrServerType = searchSolrServerType;
  }

  @JsonProperty
  public String getSearchSolrServerHttpLbservers() {
    return searchSolrServerHttpLbservers;
  }


  public void setSearchSolrServerHttpLbservers(String searchSolrServerHttpLbservers) {
    this.searchSolrServerHttpLbservers = searchSolrServerHttpLbservers;
  }

  @JsonProperty
  public Boolean getSearchSolrEmbedded() {
    return searchSolrEmbedded;
  }


  public void setSearchSolrEmbedded(Boolean searchSolrEmbedded) {
    this.searchSolrEmbedded = searchSolrEmbedded;
  }

  @JsonProperty
  public String getSearchSolrRequestHandler() {
    return searchSolrRequestHandler;
  }


  public void setSearchSolrRequestHandler(String searchSolrRequestHandler) {
    this.searchSolrRequestHandler = searchSolrRequestHandler;
  }

  @JsonProperty
  public String getSearchSolrZkHost() {
    return searchSolrZkHost;
  }


  public void setSearchSolrZkHost(String searchSolrZkHost) {
    this.searchSolrZkHost = searchSolrZkHost;
  }

  @JsonProperty
  public String getSearchSolrCollection() {
    return searchSolrCollection;
  }


  public void setSearchSolrCollection(String searchSolrCollection) {
    this.searchSolrCollection = searchSolrCollection;
  }

  @JsonProperty
  public String getDownloadWsUsername() {
    return downloadWsUsername;
  }


  public void setDownloadWsUsername(String downloadWsUsername) {
    this.downloadWsUsername = downloadWsUsername;
  }

  @JsonProperty
  public String getDownloadWsPassword() {
    return downloadWsPassword;
  }


  public void setDownloadWsPassword(String downloadWsPassword) {
    this.downloadWsPassword = downloadWsPassword;
  }

  @JsonProperty
  public String getDownloadWsUrl() {
    return downloadWsUrl;
  }


  public void setDownloadWsUrl(String downloadWsUrl) {
    this.downloadWsUrl = downloadWsUrl;
  }

  @JsonProperty
  public String getDownloadDatauseUrl() {
    return downloadDatauseUrl;
  }


  public void setDownloadDatauseUrl(String downloadDatauseUrl) {
    this.downloadDatauseUrl = downloadDatauseUrl;
  }

  @JsonProperty
  public String getDownloadWsMount() {
    return downloadWsMount;
  }


  public void setDownloadWsMount(String downloadWsMount) {
    this.downloadWsMount = downloadWsMount;
  }

  @JsonProperty
  public String getDownloadOozieUrl() {
    return downloadOozieUrl;
  }


  public void setDownloadOozieUrl(String downloadOozieUrl) {
    this.downloadOozieUrl = downloadOozieUrl;
  }

  @JsonProperty
  public String getDownloadOozieWorkflowPath() {
    return downloadOozieWorkflowPath;
  }


  public void setDownloadOozieWorkflowPath(String downloadOozieWorkflowPath) {
    this.downloadOozieWorkflowPath = downloadOozieWorkflowPath;
  }

  @JsonProperty
  public String getDownloadHiveHdfsOut() {
    return downloadHiveHdfsOut;
  }


  public void setDownloadHiveHdfsOut(String downloadHiveHdfsOut) {
    this.downloadHiveHdfsOut = downloadHiveHdfsOut;
  }

  @JsonProperty
  public String getDownloadMailSmtp() {
    return downloadMailSmtp;
  }


  public void setDownloadMailSmtp(String downloadMailSmtp) {
    this.downloadMailSmtp = downloadMailSmtp;
  }

  @JsonProperty
  public String getDownloadMailFrom() {
    return downloadMailFrom;
  }


  public void setDownloadMailFrom(String downloadMailFrom) {
    this.downloadMailFrom = downloadMailFrom;
  }

  @JsonProperty
  public String getDownloadMailBcc() {
    return downloadMailBcc;
  }


  public void setDownloadMailBcc(String downloadMailBcc) {
    this.downloadMailBcc = downloadMailBcc;
  }

  public Properties toProperties() {
    Properties properties = new Properties();
    for (Field field : this.getClass().getDeclaredFields()) {
      PropertiesKey propertiesKey = field.getAnnotation(PropertiesKey.class);
      if (propertiesKey != null) {
        try {
          final Object value = field.get(this);
          properties.put(propertiesKey.value(), value != null ? value.toString() : "");
        } catch (IllegalArgumentException e) {
          // do nothing
        } catch (IllegalAccessException e) {
          // do nothing
        }
      }
    }

    return properties;
  }

  public static void main(String[] args) throws Exception {
    Yaml yaml = new Yaml();
    OccurrenceConfiguration occurrenceConfiguration =
      yaml.loadAs(Resources.getResource("occurrence.yaml").openStream(), OccurrenceConfiguration.class);
    System.out.println(occurrenceConfiguration.toProperties());
  }
}
