package org.gbif.occurrence.download.inject;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.search.inject.SolrModule;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileCoordinator;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileSupervisor;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.dwca.DwcaOccurrenceDownloadFileCoordinator;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvOccurrenceDownloadFileCoordinator;
import org.gbif.occurrence.download.oozie.DownloadPrepareStep;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.zookeeper.ZooKeeperLockFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;

import akka.dispatch.ExecutionContextExecutorService;
import akka.dispatch.ExecutionContexts;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Private guice module that provides bindings the required Modules and dependencies.
 * The following class are exposed:
 * - CuratorFramework: this class is exposed only to close the zookeeper connections properly.
 * - OccurrenceFileWriter: class that creates the occurrence data and citations file.
 */
public final class DownloadWorkflowModule extends AbstractModule {

  public static final String CONF_FILE = "occurrence-download.properties";

  //Prefix for static settings
  public static final String PROPERTIES_PREFIX = "occurrence.download.";

  private static final String LOCKING_PATH = "/runningJobs/";

  private final Properties properties;

  /**
   * Utility class that contains constants and keys set each time the worklfow is executed.
   */
  public static class DynamicSettings {

    /**
     * Hidden constructor.
     */
    private DynamicSettings(){
      //empty
    }

    //Prefix for dynamic settings usually set each time that workflow is executed
    public static final String WORKFLOW_PROPERTIES_PREFIX = PROPERTIES_PREFIX + "workflow.";

    public static final String DOWNLOAD_FORMAT_KEY = WORKFLOW_PROPERTIES_PREFIX + "format";

    public static final String DOWNLOAD_KEY = WORKFLOW_PROPERTIES_PREFIX + "downloadKey";

    public static final String HDFS_OUPUT_PATH_KEY = WORKFLOW_PROPERTIES_PREFIX + "hdfsOutputPath";

  }

  /**
   * Utility class that contains configuration keys of common settings.
   */
  public static class DefaultSettings {

    /**
     * Hidden constructor.
     */
    private DefaultSettings(){
      //empty
    }

    public static final String NAME_NODE_KEY =  "hdfs.namenode";
    public static final String REGISTRY_URL_KEY = "registry.ws.url";

    public static final String MAX_THREADS_KEY = PROPERTIES_PREFIX + "job.max_threads";
    public static final String JOB_MIN_RECORDS_KEY = PROPERTIES_PREFIX + "job.min_records";
    public static final String MAX_RECORDS_KEY = PROPERTIES_PREFIX + "file.max_records";
    public static final String ZK_LOCK_NAME_KEY = PROPERTIES_PREFIX + "zookeeper.lock_name";
    public static final String OCC_HBASE_TABLE_KEY = "hbase.table";
    public static final String DOWNLOAD_USER_KEY = PROPERTIES_PREFIX + "ws.username";
    public static final String DOWNLOAD_PASSWORD_KEY = PROPERTIES_PREFIX + "ws.password";

  }

  /**
   * Loads the default configuration file name and copies the additionalProperties into it.
   * @param additionalProperties configuration settings
   */
  public DownloadWorkflowModule(Properties additionalProperties) {
    try {
      properties = PropertiesUtil.loadProperties(CONF_FILE);
      properties.putAll(additionalProperties);
    } catch(Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  /**
   * Loads the default configuration file name 'occurrence-download.properties'.
   */
  public DownloadWorkflowModule() {
    try {
      properties = PropertiesUtil.loadProperties(CONF_FILE);
    } catch(Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  @Override
  protected void configure() {
    Names.bindProperties(binder(), properties);
    install(new SolrModule());
    bind(OccurrenceMapReader.class);
    bind(DownloadPrepareStep.class);
    bindDownloadFilesBuilding();
  }

  /**
   * Binds a DownloadFilesAggregator according to the DownloadFormat set using the key DOWNLOAD_FORMAT_KEY.
   */
  private void bindDownloadFilesBuilding(){
    if(properties.containsKey(DynamicSettings.DOWNLOAD_FORMAT_KEY)){
      bind(OccurrenceDownloadFileSupervisor.Configuration.class);
      bind(OccurrenceDownloadFileSupervisor.class);
      DownloadFormat downloadFormat = DownloadFormat.valueOf(properties.getProperty(DynamicSettings.DOWNLOAD_FORMAT_KEY));
      if (DownloadFormat.DWCA == downloadFormat) {
        bind(OccurrenceDownloadFileCoordinator.class).to(DwcaOccurrenceDownloadFileCoordinator.class);
      } else if (DownloadFormat.SIMPLE_CSV == downloadFormat) {
        bind(OccurrenceDownloadFileCoordinator.class).to(SimpleCsvOccurrenceDownloadFileCoordinator.class);
      }
    }
  }



  @Provides
  @Singleton
  CuratorFramework provideCuratorFramework(@Named(PROPERTIES_PREFIX + "zookeeper.namespace") String zookeeperNamespace,
    @Named(PROPERTIES_PREFIX + "zookeeper.quorum") String zookeeperConnection,
    @Named(PROPERTIES_PREFIX + "zookeeper.sleep_time") Integer sleepTime,
    @Named(PROPERTIES_PREFIX + "zookeeper.max_retries") Integer maxRetries)
    throws IOException {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .namespace(zookeeperNamespace)
      .retryPolicy(new ExponentialBackoffRetry(sleepTime, maxRetries))
      .connectString(zookeeperConnection)
      .build();
    curator.start();
    return curator;
  }

  @Provides
  @Singleton
  DatasetOccurrenceDownloadUsageService provideDatasetOccurrenceDownloadUsageService(@Named(DefaultSettings.REGISTRY_URL_KEY) String registryWsUri) {
    RegistryClientUtil registryClientUtil = new RegistryClientUtil(properties);
    return registryClientUtil.setupDatasetUsageService(registryWsUri);
  }

  @Provides
  @Singleton
  DatasetService provideDatasetService(@Named(DefaultSettings.REGISTRY_URL_KEY) String registryWsUri) {
    RegistryClientUtil registryClientUtil = new RegistryClientUtil(properties);
    return registryClientUtil.setupDatasetService(registryWsUri);
  }

  @Provides
  @Singleton
  OccurrenceDownloadService provideOccurrenceDownloadService(@Named(DefaultSettings.REGISTRY_URL_KEY) String registryWsUri) {
    RegistryClientUtil registryClientUtil = new RegistryClientUtil(properties);
    return registryClientUtil.setupOccurrenceDownloadService(registryWsUri);
  }

  @Provides
  ExecutionContextExecutorService provideExecutionContextExecutorService(
    @Named(PROPERTIES_PREFIX + "job.max_threads") int maxThreads) {
    return ExecutionContexts.fromExecutorService(Executors.newFixedThreadPool(maxThreads));
  }

  @Provides
  HTablePool provideHTablePool(@Named(PROPERTIES_PREFIX + "max_connection_pool") Integer maxConnectionPool) {
    return new HTablePool(HBaseConfiguration.create(), maxConnectionPool);
  }

  @Provides
  LockFactory provideLock(CuratorFramework curatorFramework,
    @Named(PROPERTIES_PREFIX + "max_global_threads") Integer maxGlobalThreads) {
    return new ZooKeeperLockFactory(curatorFramework, maxGlobalThreads, LOCKING_PATH);
  }

}
