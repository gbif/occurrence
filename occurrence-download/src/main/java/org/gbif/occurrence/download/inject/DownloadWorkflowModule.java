package org.gbif.occurrence.download.inject;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import lombok.Builder;
import lombok.Data;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.DownloadMaster;
import org.gbif.occurrence.download.file.dwca.DwcaDownloadAggregator;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvDownloadAggregator;
import org.gbif.occurrence.download.file.specieslist.SpeciesListDownloadAggregator;
import org.gbif.occurrence.download.oozie.DownloadPrepareAction;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.occurrence.search.es.EsConfig;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

/**
 * Utility factory class to create instances of common complex objects required by Download Actions.
 */
@Data
@Builder
public class DownloadWorkflowModule  {

  public static final String CONF_FILE = "occurrence-download.properties";

  //Prefix for static settings
  public static final String PROPERTIES_PREFIX = "occurrence.download.";

  private static final String ES_PREFIX = "es.";

  private final WorkflowConfiguration workflowConfiguration;

  private final DownloadJobConfiguration downloadJobConfiguration;

  /**
   * DownloadPrepare action factory method.
   * This is the initial action that counts records and its output is used to decide if a download is processed through Hive or Es.
   */
  public DownloadPrepareAction downloadPrepareAction() {

    //Using the registry client util because it configures the retry mechanism

    return DownloadPrepareAction.builder().esClient(esClient())
            .esIndex(workflowConfiguration.getSetting(DefaultSettings.ES_INDEX_KEY))
            .smallDownloadLimit(workflowConfiguration.getIntSetting(DefaultSettings.MAX_RECORDS_KEY))
            .workflowConfiguration(workflowConfiguration)
            .occurrenceDownloadService(registryClientUtil().setupOccurrenceDownloadService())
            .build();
  }

  public RegistryClientUtil registryClientUtil() {
    return new RegistryClientUtil(workflowConfiguration.getSetting(DefaultSettings.DOWNLOAD_USER_KEY),
                                  workflowConfiguration.getSetting(DefaultSettings.DOWNLOAD_PASSWORD_KEY),
                                  workflowConfiguration.getSetting(DefaultSettings.REGISTRY_URL_KEY));
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
      .connectString( workflowConfiguration.getSetting(DefaultSettings.ZK_QUORUM_KEY))
      .build();

    curator.start();
    return curator;
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

  /**
   * Creates an ActorRef that holds an instance of {@link DownloadMaster}.
   */
  public ActorRef downloadMaster(ActorSystem system) {
    return system.actorOf(new Props(() -> new DownloadMaster(workflowConfiguration,
                                                       masterConfiguration(),
                                                       esClient(),
                                                       workflowConfiguration.getSetting(DefaultSettings.ES_INDEX_KEY),
                                                       downloadJobConfiguration,
                                                       getAggregator(),
                                                       workflowConfiguration.getIntSetting(DefaultSettings.MAX_GLOBAL_THREADS_KEY))),
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
          return new DwcaDownloadAggregator(downloadJobConfiguration,
                                            registryClientUtil().setupOccurrenceDownloadService());

        case SIMPLE_CSV:
          return new SimpleCsvDownloadAggregator(downloadJobConfiguration,
                                                 workflowConfiguration,
                                                 registryClientUtil().setupOccurrenceDownloadService());

        case SPECIES_LIST:
          return new SpeciesListDownloadAggregator(downloadJobConfiguration,
                                                   workflowConfiguration,
                                                   registryClientUtil().setupOccurrenceDownloadService());
        case SIMPLE_AVRO:
        case SIMPLE_WITH_VERBATIM_AVRO:
        case IUCN:
        case MAP_OF_LIFE:
        case BIONOMIA:
        default:
          return new NotSupportedDownloadAggregator();
      }
    }
    throw new IllegalStateException("Unknown download format '" + downloadFormat + "'.");
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
  public static final class DefaultSettings {

    public static final String NAME_NODE_KEY = "hdfs.namenode";
    public static final String HIVE_DB_KEY = "hive.db";
    public static final String REGISTRY_URL_KEY = "registry.ws.url";
    public static final String API_URL_KEY = "api.url";
    public static final String ES_INDEX_KEY = "es.index";
    public static final String ES_HOSTS_KEY = "es.hosts";
    public static final String ES_CONNECT_TIMEOUT_KEY = "es.connect_timeout";
    public static final String ES_SOCKET_TIMEOUT_KEY = "es.socket_timeout";
    public static final String ES_SNIFF_INTERVAL_KEY = "es.sniff_interval";
    public static final String ES_SNIFF_AFTER_FAILURE_DELAY_KEY = "es.sniff_after_failure_delay";

    /**
     * Hidden constructor.
     */
    private DefaultSettings() {
      //empty
    }
    public static final String MAX_THREADS_KEY = PROPERTIES_PREFIX + "job.max_threads";
    public static final String JOB_MIN_RECORDS_KEY = PROPERTIES_PREFIX + "job.min_records";
    public static final String MAX_RECORDS_KEY = PROPERTIES_PREFIX + "file.max_records";
    public static final String ZK_LOCK_NAME_KEY = PROPERTIES_PREFIX + "zookeeper.lock_name";
    public static final String MAX_GLOBAL_THREADS_KEY = PROPERTIES_PREFIX + "max_global_threads";

    public static final String DOWNLOAD_USER_KEY = PROPERTIES_PREFIX + "ws.username";
    public static final String DOWNLOAD_PASSWORD_KEY = PROPERTIES_PREFIX + "ws.password";
    public static final String DOWNLOAD_LINK_KEY = PROPERTIES_PREFIX + "link";
    public static final String HDFS_OUTPUT_PATH_KEY = PROPERTIES_PREFIX + "hdfsOutputPath";
    public static final String TMP_DIR_KEY = PROPERTIES_PREFIX + "tmp.dir";
    public static final String HIVE_DB_PATH_KEY = PROPERTIES_PREFIX + "hive.hdfs.out";


    public static final String ZK_INDICES_NS_KEY = PROPERTIES_PREFIX + "zookeeper.indices.namespace";
    public static final String ZK_DOWNLOADS_NS_KEY = PROPERTIES_PREFIX + "zookeeper.downloads.namespace";
    public static final String ZK_QUORUM_KEY = PROPERTIES_PREFIX + "zookeeper.quorum";
    public static final String ZK_SLEEP_TIME_KEY = PROPERTIES_PREFIX + "zookeeper.sleep_time";
    public static final String ZK_MAX_RETRIES_KEY = PROPERTIES_PREFIX + "zookeeper.max_retries";

  }

}
