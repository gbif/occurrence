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
package org.gbif.occurrence.download.inject;

import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.event.search.es.EventEsField;
import org.gbif.event.search.es.SearchHitEventConverter;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.DownloadMaster;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.dwca.akka.DwcaDownloadAggregator;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvDownloadAggregator;
import org.gbif.occurrence.download.file.specieslist.SpeciesListDownloadAggregator;
import org.gbif.occurrence.download.oozie.DownloadPrepareAction;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.es.SearchHitConverter;
import org.gbif.occurrence.search.es.SearchHitOccurrenceConverter;
import org.gbif.registry.ws.client.EventDownloadClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.wrangler.lock.Mutex;
import org.gbif.wrangler.lock.ReadWriteMutexFactory;
import org.gbif.wrangler.lock.zookeeper.ZookeeperSharedReadWriteMutex;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.function.Function;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.UtilityClass;

;

/**
 * Utility factory class to create instances of common complex objects required by Download Actions.
 */
@Data
@Builder
public class DownloadWorkflowModule  {

  public static final String CONF_FILE = "download.properties";

  //Prefix for static settings
  public static final String PROPERTIES_PREFIX = "occurrence.download.";

  private static final String ES_PREFIX = "es.";

  private static final String INDEX_LOCKING_PATH = "/indices/";

  private final WorkflowConfiguration workflowConfiguration;

  private final DownloadJobConfiguration downloadJobConfiguration;

  /**
   * DownloadPrepare action factory method.
   * This is the initial action that counts records and its output is used to decide if a download is processed through Hive or Es.
   */
  public DownloadPrepareAction downloadPrepareAction(DwcTerm dwcTerm, String wfPath) {
    return DownloadPrepareAction.builder().esClient(esClient())
            .esIndex(workflowConfiguration.getSetting(DefaultSettings.ES_INDEX_KEY))
            .smallDownloadLimit(workflowConfiguration.getIntSetting(DefaultSettings.MAX_RECORDS_KEY))
            .workflowConfiguration(workflowConfiguration)
            .occurrenceDownloadService(downloadServiceClient(dwcTerm))
            .occurrenceBaseEsFieldMapper(fieldMapper())
            .coreTerm(dwcTerm)
            .wfPath(wfPath)
            .build();
  }

  private OccurrenceBaseEsFieldMapper fieldMapper() {
    return WorkflowConfiguration.SearchType.OCCURRENCE == workflowConfiguration.getEsIndexType()? OccurrenceEsField.buildFieldMapper() : EventEsField.buildFieldMapper();
  }

  /**
   * Creates a DownloadService for Event or Occurrence downloads.
   */
  private OccurrenceDownloadService downloadServiceClient(DwcTerm coreTerm) {
    return DwcTerm.Event == coreTerm? clientBuilder().build(EventDownloadClient.class) : clientBuilder().build(OccurrenceDownloadClient.class);
  }

  /**
   * GBIF Ws client factory.
   */
  public ClientBuilder clientBuilder() {
    return new ClientBuilder()
        .withUrl(workflowConfiguration.getSetting(DefaultSettings.REGISTRY_URL_KEY))
        .withCredentials(
            workflowConfiguration.getSetting(DefaultSettings.DOWNLOAD_USER_KEY),
            workflowConfiguration.getSetting(DefaultSettings.DOWNLOAD_PASSWORD_KEY))
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder();
  }

  /**
   * Creates am started CuratorFramework instance using the settings in workflowConfiguration.
   */
  public  CuratorFramework curatorFramework() {
      return curatorFramework(workflowConfiguration);
  }

  /**
   * Creates and started CuratorFramework using the provided configuration.
   */
  public static CuratorFramework curatorFramework(WorkflowConfiguration workflowConfiguration) {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .namespace(workflowConfiguration.getSetting(DefaultSettings.ZK_DOWNLOADS_NS_KEY))
      .retryPolicy(new ExponentialBackoffRetry(workflowConfiguration.getIntSetting(DefaultSettings.ZK_SLEEP_TIME_KEY),
                                               workflowConfiguration.getIntSetting(DefaultSettings.ZK_MAX_RETRIES_KEY)))
      .connectString(workflowConfiguration.getSetting(DefaultSettings.ZK_QUORUM_KEY))
      .build();

    curator.start();
    return curator;
  }

  /**
   * Creates a RW Mutex, used later to create a mutex to synchronize modification to the ES index.
   */
  public Mutex provideReadLock(CuratorFramework curatorFramework) {
    ReadWriteMutexFactory readWriteMutexFactory = new ZookeeperSharedReadWriteMutex(curatorFramework, INDEX_LOCKING_PATH);
    return readWriteMutexFactory
            .createReadMutex(workflowConfiguration.getSetting(DefaultSettings.ES_INDEX_KEY));
  }

  /**
   * Factory method for Elasticsearch client.
   */
  public RestHighLevelClient esClient() {
    EsConfig esConfig = EsConfig.fromProperties(workflowConfiguration.getDownloadSettings(), ES_PREFIX);
    HttpHost[] hosts = new HttpHost[esConfig.getHosts().length];
    int i = 0;
    for (String host : esConfig.getHosts()) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    SniffOnFailureListener sniffOnFailureListener =
      new SniffOnFailureListener();

    RestClientBuilder builder =
      RestClient.builder(hosts)
        .setRequestConfigCallback(
          requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(esConfig.getConnectTimeout())
            .setSocketTimeout(esConfig.getSocketTimeout()))
        .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

    if (esConfig.getSniffInterval() > 0) {
      builder.setFailureListener(sniffOnFailureListener);
    }

    RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

    if (esConfig.getSniffInterval() > 0) {
      Sniffer sniffer = Sniffer.builder(highLevelClient.getLowLevelClient())
        .setSniffIntervalMillis(esConfig.getSniffInterval())
        .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
        .build();
      sniffOnFailureListener.setSniffer(sniffer);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        sniffer.close();
        try {
          highLevelClient.close();
        } catch (IOException e) {
          throw new IllegalStateException("Couldn't close ES client", e);
        }
      }));
    }

    return highLevelClient;
  }

  public OccurrenceBaseEsFieldMapper esFieldMapper() {
    return WorkflowConfiguration.SearchType.EVENT == workflowConfiguration.getEsIndexType()? EventEsField.buildFieldMapper() : OccurrenceEsField.buildFieldMapper();
  }

  /**
   *  Configuration for the DownloadMater actor.
   */
  private DownloadMaster.MasterConfiguration masterConfiguration() {
    return  DownloadMaster.MasterConfiguration.builder()
            .nrOfWorkers(workflowConfiguration.getIntSetting(DefaultSettings.MAX_THREADS_KEY))
            .minNrOfRecords(workflowConfiguration.getIntSetting(DefaultSettings.JOB_MIN_RECORDS_KEY))
            .maximumNrOfRecords(workflowConfiguration.getIntSetting(DefaultSettings.MAX_RECORDS_KEY))
            .lockName(workflowConfiguration.getSetting(DefaultSettings.ZK_LOCK_NAME_KEY))
            .build();
  }

  private <T extends VerbatimOccurrence, S extends SearchHitConverter<T>> S searchHitConverter() {
    if (workflowConfiguration.getEsIndexType() == WorkflowConfiguration.SearchType.EVENT) {
      return (S) new SearchHitEventConverter(esFieldMapper(), false);
    }
    return (S) new SearchHitOccurrenceConverter(esFieldMapper(), false);
  }

  private <T extends VerbatimOccurrence> Function<T, Map<String,String>> verbatimMapper(){
    return  OccurrenceMapReader::buildVerbatimOccurrenceMap;
  }

  private <T extends VerbatimOccurrence> Function<T, Map<String,String>> interpreterMapper() {
    if (workflowConfiguration.getEsIndexType() == WorkflowConfiguration.SearchType.EVENT) {
      return (T record) -> OccurrenceMapReader.buildInterpretedEventMap((Event)record);
    }
    return  (T record) -> OccurrenceMapReader.buildInterpretedOccurrenceMap((Occurrence) record);
  }

  /**
   * Creates an ActorRef that holds an instance of {@link DownloadMaster}.
   */
  public ActorRef downloadMaster(ActorSystem system) {
    return system.actorOf(new Props(() -> DownloadMaster.builder()
                                            .workflowConfiguration(workflowConfiguration)
                                            .masterConfiguration(masterConfiguration())
                                            .esClient(esClient())
                                            .esIndex(workflowConfiguration.getSetting(DefaultSettings.ES_INDEX_KEY))
                                            .jobConfiguration(downloadJobConfiguration)
                                            .aggregator(getAggregator())
                                            .maxGlobalJobs(workflowConfiguration.getIntSetting(DefaultSettings.MAX_GLOBAL_THREADS_KEY))
                                            .interpretedMapper(interpreterMapper())
                                            .verbatimMapper(verbatimMapper())
                                            .searchHitConverter(searchHitConverter())
                                            .build()),
                          "DownloadMaster" + downloadJobConfiguration.getDownloadKey());
  }

  /**
   * Binds a DownloadFilesAggregator according to the DownloadFormat set using the key DOWNLOAD_FORMAT_KEY.
   */

  public DownloadAggregator getAggregator() {
    DownloadFormat downloadFormat = workflowConfiguration.getDownloadFormat();

    if (downloadFormat != null) {
      switch (downloadFormat) {
        case DWCA:
          return new DwcaDownloadAggregator(downloadJobConfiguration);

        case SIMPLE_CSV:
          return new SimpleCsvDownloadAggregator(downloadJobConfiguration,
                                                 workflowConfiguration,
                                                 clientBuilder().build(OccurrenceDownloadClient.class));

        case SPECIES_LIST:
          return new SpeciesListDownloadAggregator(downloadJobConfiguration,
                                                   workflowConfiguration,
                                                   clientBuilder().build(OccurrenceDownloadClient.class));
        case SIMPLE_AVRO:
        case SIMPLE_PARQUET:
        case SIMPLE_WITH_VERBATIM_AVRO:
        case IUCN:
        case MAP_OF_LIFE:
        case BIONOMIA:
        default:
          return new NotSupportedDownloadAggregator();
      }
    }
    throw new IllegalStateException("Download format not specified");
  }

  /**
   * Utility class that contains constants and keys set each time the workflow is executed.
   */
   public static final class DynamicSettings {

    /**
     * Hidden constructor.
     */
    private DynamicSettings() {
      //empty
    }

    //Prefix for dynamic settings usually set each time that workflow is executed
    public static final String WORKFLOW_PROPERTIES_PREFIX = PROPERTIES_PREFIX + "workflow.";

    public static final String DOWNLOAD_FORMAT_KEY = WORKFLOW_PROPERTIES_PREFIX + "format";

  }

  /**
   * Utility class that contains configuration keys of common settings.
   */
  @UtilityClass
  public static final class DefaultSettings {

    public static final String NAME_NODE_KEY = "hdfs.namenode";
    public static final String HIVE_DB_KEY = "hive.db";
    public static final String REGISTRY_URL_KEY = "registry.ws.url";
    public static final String API_URL_KEY = "api.url";
    public static final String ES_INDEX_KEY = "es.index";
    public static final String ES_HOSTS_KEY = "es.hosts";
    public static final String ES_INDEX_TYPE = "es.index.type";
    public static final String ES_INDEX_NESTED = "es.index.nested";
    public static final String ES_CONNECT_TIMEOUT_KEY = "es.connect_timeout";
    public static final String ES_SOCKET_TIMEOUT_KEY = "es.socket_timeout";
    public static final String ES_SNIFF_INTERVAL_KEY = "es.sniff_interval";
    public static final String ES_SNIFF_AFTER_FAILURE_DELAY_KEY = "es.sniff_after_failure_delay";

    public static final String MAX_THREADS_KEY = PROPERTIES_PREFIX + "job.max_threads";
    public static final String JOB_MIN_RECORDS_KEY = PROPERTIES_PREFIX + "job.min_records";
    public static final String MAX_RECORDS_KEY = PROPERTIES_PREFIX + "file.max_records";
    public static final String ZK_LOCK_NAME_KEY = PROPERTIES_PREFIX + "zookeeper.lock_name";
    public static final String MAX_GLOBAL_THREADS_KEY = PROPERTIES_PREFIX + "max_global_threads";

    public static final String DOWNLOAD_USER_KEY = PROPERTIES_PREFIX + "ws.username";
    public static final String DOWNLOAD_PASSWORD_KEY = PROPERTIES_PREFIX + "ws.password";
    public static final String DOWNLOAD_LINK_KEY = PROPERTIES_PREFIX + "link";
    public static final String HDFS_OUTPUT_PATH_KEY = PROPERTIES_PREFIX + "hdfsOutputPath";
    public static final String HDFS_TMP_DIR_KEY = PROPERTIES_PREFIX + "hdfs.tmp.dir";
    public static final String TMP_DIR_KEY = PROPERTIES_PREFIX + "tmp.dir";
    public static final String HIVE_DB_PATH_KEY = PROPERTIES_PREFIX + "hive.hdfs.out";

    public static final String ZK_INDICES_NS_KEY = PROPERTIES_PREFIX + "zookeeper.indices.namespace";
    public static final String ZK_DOWNLOADS_NS_KEY = PROPERTIES_PREFIX + "zookeeper.downloads.namespace";
    public static final String ZK_QUORUM_KEY = PROPERTIES_PREFIX + "zookeeper.quorum";
    public static final String ZK_SLEEP_TIME_KEY = PROPERTIES_PREFIX + "zookeeper.sleep_time";
    public static final String ZK_MAX_RETRIES_KEY = PROPERTIES_PREFIX + "zookeeper.max_retries";

  }

}
