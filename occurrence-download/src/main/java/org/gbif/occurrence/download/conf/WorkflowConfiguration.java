package org.gbif.occurrence.download.conf;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * Occurrence download workflow configuration.
 * Contains the configuration settings that are accessible for all the executions of the download workflow.
 * The configuration setting are read from the file DownloadWorkflowModule.CONF_FILE or are passed as constructor
 * parameter as java Properties class.
 */
public class WorkflowConfiguration {

  private final Properties settings;
  private final Configuration hadoopConf;

  /**
   *
   * @param settings properties class load with the workflow settings
   */
  public WorkflowConfiguration(Properties settings) {
    this.settings = new Properties(settings);
    hadoopConf = new Configuration();
    hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsNameNode());
  }

  /**
   * Loads the configuration from the file DownloadWorkflowModule.CONF_FILE.
   */
  public WorkflowConfiguration() {
    try {
      settings = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
      hadoopConf = new Configuration();
      hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsNameNode());
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  /**
   *
   * @return hdfs name node
   */
  public String getHdfsNameNode() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY);
  }

  /**
   *
   * @return hive database name
   */
  public String getHiveDb() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HIVE_DB_KEY);
  }

  /**
   *
   * @return  registry API url
   */
  public String getRegistryWsUrl() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY);
  }

  /**
   *
   * @return local temp dir where downloads files are created
   */
  public String getTempDir() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.TMP_DIR_KEY);
  }

  /**
   *
   * @param downloadKey download id
   * @return a link to the download file
   */
  public String getDownloadLink(String downloadKey) {
    Preconditions.checkNotNull(settings);
    Preconditions.checkNotNull(downloadKey);
    // download link needs to be constructed
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_LINK_KEY)
      .replace(DownloadUtils.DOWNLOAD_ID_PLACEHOLDER, downloadKey);
  }

  /**
   *
   * @return hdfs directory where hive stores tables
   */
  public String getHdfsOutputPath() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HDFS_OUTPUT_PATH_KEY);
  }

  /**
   *
   * @return hdfs directory of the Hive database
   */
  public String getHiveDBPath() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HIVE_DB_PATH_KEY);
  }

  /**
   *
   * @return requested download format this value change for each execution
   */
  public DownloadFormat getDownloadFormat() {
    Preconditions.checkNotNull(settings);
    String downloadFormat = settings.getProperty(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY);
    if (downloadFormat != null) {
      return DownloadFormat.valueOf(downloadFormat);
    }
    return null;
  }

  /**
   *
   * @return GBIF API url
   */
  public String getApiUrl() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.API_URL_KEY);
  }

  /**
   *
   * @return HDFS Hadoop configuration
   */
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  /**
   *
   * @return properties class that contains the raw configuration settings
   */
  public Properties getDownloadSettings() {
    return settings;
  }
}
