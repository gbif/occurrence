package org.gbif.occurrence.download.file.common;

import java.io.IOException;
import java.util.Properties;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import com.google.common.base.Preconditions;

/**
 * 
 * Oozie action persists occurrence download count information to registry.
 *
 */
public class DownloadCountPersistence {

  public static void main(String[] args) throws IOException{
    String countPath = Preconditions.checkNotNull(args[0]);
    String downloadKey = Preconditions.checkNotNull(args[1]);
    
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    String nameNode = properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY);
    String registryWsURL = properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY);
  
    RegistryClientUtil registryClientUtil = new RegistryClientUtil();
    OccurrenceDownloadService occurrenceDownloadService = registryClientUtil.setupOccurrenceDownloadService(registryWsURL);
    // persists species count information.
    DownloadCount.persist(downloadKey, DownloadFileUtils.readCount(nameNode, countPath), occurrenceDownloadService);
  }
}
