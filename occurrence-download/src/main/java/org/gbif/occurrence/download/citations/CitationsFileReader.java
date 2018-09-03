package org.gbif.occurrence.download.citations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * Reads a datasets citations file and optionally persists the data usages and return the usages into a Map object.
 */
public final class CitationsFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CitationsFileReader.class);
  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();

  /**
   * Transforms tab-separated-line into a DatasetOccurrenceDownloadUsage instance.
   */
  private static SimpleImmutableEntry<UUID,Long> toDatasetOccurrenceDownloadUsage(String tsvLine, String downloadKey) {
    Iterator<String> tsvLineIterator = TAB_SPLITTER.split(tsvLine).iterator();
    return new AbstractMap.SimpleImmutableEntry<UUID,Long>(UUID.fromString(tsvLineIterator.next()),Long.parseLong(tsvLineIterator.next()) );
  }

  /**
   * Reads a dataset citations file with the form 'datasetkeyTABnumberOfRecords' and applies the listed predicates.
   * Each line in read from the TSV file is transformed into a DatasetOccurrenceDownloadUsage.
   *
   * @param nameNode     Hadoop name node uri
   * @param citationPath path to the directory that contains the citation table files
   * @param downloadKey  occurrence download key
   * @param predicates   list of predicates to apply while reading the file
   */
  public static void readCitations(String nameNode, String citationPath,String downloadKey,
                                   Predicate<Map<UUID,Long>> predicate) throws IOException {
    Map<UUID,Long> datasetsCitation = new HashMap<>();
    FileSystem hdfs = DownloadFileUtils.getHdfs(nameNode);
    for (FileStatus fs : hdfs.listStatus(new Path(citationPath))) {
      if (!fs.isDirectory()) {
        try (BufferedReader citationReader = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath()),
                                                                                      Charsets.UTF_8))) {
          for (String tsvLine = citationReader.readLine(); tsvLine != null; tsvLine = citationReader.readLine()) {
            if (!Strings.isNullOrEmpty(tsvLine)) {
              // prepare citation object and add it to list
                  SimpleImmutableEntry<UUID, Long> citationEntry = toDatasetOccurrenceDownloadUsage(tsvLine, downloadKey);
                  datasetsCitation.put(citationEntry.getKey(), citationEntry.getValue());
            }
          }
        }
      }
    }
    predicate.apply(datasetsCitation);
  }

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    readCitations(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY),
                  Preconditions.checkNotNull(args[0]),
                  Preconditions.checkNotNull(args[1]),
                  new PersistUsage(Preconditions.checkNotNull(args[1]),properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY)));
  }

  /**
   * Private constructor.
   */
  private CitationsFileReader() {
    //empty constructor
  }

  /**
   * Persists the dataset usage into the Registry data base.
   */
  public static class PersistUsage implements Predicate<Map<UUID,Long>> {

    private final String downloadKey;

    private final OccurrenceDownloadService downloadService;

    public PersistUsage(String registryWsUrl,String downloadKey) {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil();
      this.downloadKey = downloadKey; 
      this.downloadService = registryClientUtil.setupOccurrenceDownloadService(registryWsUrl);
    }

    public PersistUsage(String downloadKey, OccurrenceDownloadService downloadService) {
      this.downloadKey = downloadKey;
      this.downloadService = downloadService;
    }

    @Override
    public boolean apply(@Nullable Map<UUID,Long> datasetsCitation) {
      if(datasetsCitation==null || datasetsCitation.isEmpty()) {
        LOG.info("No citation information to update as list of datasets is empty or null, hence ignoring the request");
        return true;
      }
      try {
        downloadService.createUsages(downloadKey, datasetsCitation);
      } catch (Exception e) {
        LOG.error("Error persisting dataset usage information {}", datasetsCitation, e);
        return false;
      }
      return true;
    }
  }
}
