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
package org.gbif.occurrence.download.conf;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.search.es.EsFieldMapper;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

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
    this.settings = new Properties();
    this.settings.putAll(settings);
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
   * @return HDFS temp dir where downloads files are created
   */
  public String getHdfsTempDir() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HDFS_TMP_DIR_KEY);
  }

  /**
   *
   * @return is the index nested
   */
  public Boolean isEsNestedIndex() {
    Preconditions.checkNotNull(settings);
    return getBoolSetting(DownloadWorkflowModule.DefaultSettings.ES_INDEX_NESTED);
  }

  /**
   *
   * @return Index object type to use as main object
   */
  public EsFieldMapper.SearchType getEsIndexType() {
    Preconditions.checkNotNull(settings);
    return EsFieldMapper.SearchType.valueOf(settings.getProperty(DownloadWorkflowModule.DefaultSettings.ES_INDEX_TYPE).toUpperCase());
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

  public String getSetting(String key) {
    return settings.getProperty(key);
  }

  public Integer getIntSetting(String key) {
    return Integer.parseInt(settings.getProperty(key));
  }

  public Boolean getBoolSetting(String key) {
    return Boolean.parseBoolean(settings.getProperty(key));
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
