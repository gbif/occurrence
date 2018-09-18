package org.gbif.occurrence.download.file.specieslist;

import java.io.IOException;
import java.util.Properties;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.citations.CitationsFileReader;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import com.google.common.base.Preconditions;
/**
 * 
 * Oozie action persists meta information of species list download to registry.
 *
 */
public class SpeciesDownloadMetaPersistence {

  public static void main(String[] args) throws IOException {
    String countPath = Preconditions.checkNotNull(args[0]);
    String downloadKey = Preconditions.checkNotNull(args[1]);
    String citationPath = Preconditions.checkNotNull(args[2]);
    
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    String nameNode = properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY);
    String registryWsURL = properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY);
    // persists citation information.
    CitationsFileReader.readCitations(nameNode, citationPath, new CitationsFileReader.PersistUsage(downloadKey, registryWsURL));
    
    RegistryClientUtil registryClientUtil = new RegistryClientUtil();
    OccurrenceDownloadService occurrenceDownloadService = registryClientUtil.setupOccurrenceDownloadService(registryWsURL);
    // persists species count information.
    SpeciesCount.persist(downloadKey, DownloadFileUtils.readSpeciesCount(nameNode, countPath), occurrenceDownloadService);
  }

}
