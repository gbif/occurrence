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

public class WorkflowConfiguration {

  private final Properties settings;
  private final Configuration hadoopConf;

  public WorkflowConfiguration(Properties settings) {
    this.settings = settings;
    hadoopConf = new Configuration();
    hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsNameNode());
  }

  public WorkflowConfiguration() {
    try {
      settings = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
      hadoopConf = new Configuration();
      hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsNameNode());
    } catch (IOException ex){
      throw Throwables.propagate(ex);
    }
  }
  public String getHdfsNameNode(){
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY);
  }

  public String getHiveDb(){
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HIVE_DB_KEY);
  }

  public String getRegistryWsUrl() {
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY);
  }

  public String getTempDir(){
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.TMP_DIR_KEY);
  }


  public String getDownloadLink(String downloadKey){
    Preconditions.checkNotNull(settings);
    Preconditions.checkNotNull(downloadKey);
    // download link needs to be constructed
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_LINK_KEY)
      .replace(DownloadUtils.DOWNLOAD_ID_PLACEHOLDER, downloadKey);
  }

  public String getHdfsOutputPath(){
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HDFS_OUTPUT_PATH_KEY);
  }

  public String getHiveDBPath(){
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.HIVE_DB_PATH_KEY);
  }

  public DownloadFormat getDownloadFormat(){
    Preconditions.checkNotNull(settings);
    String downloadFormat = settings.getProperty(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY);
    if(downloadFormat != null){
      return DownloadFormat.valueOf(downloadFormat);
    }
    return null;
  }

  public String getApiUrl(){
    Preconditions.checkNotNull(settings);
    return settings.getProperty(DownloadWorkflowModule.DefaultSettings.API_URL_KEY);
  }

  public Configuration getHadoopConf(){
    return hadoopConf;
  }

  public Properties getDownloadSettings(){
    return settings;
  }
}
