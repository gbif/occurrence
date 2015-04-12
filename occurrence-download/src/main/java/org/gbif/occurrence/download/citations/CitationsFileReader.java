package org.gbif.occurrence.download.citations;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a datasets citations file and optionally persists teh data usages and return the usages into a Map object.
 */
public class CitationsFileReader {

  /**
   * Predicates that copies the DatasetUsage into a Map.
   */
  public static class UsageToMap implements Predicate<DatasetOccurrenceDownloadUsage> {

    private Map<UUID,Long> usages;

    /**
     * @param usages where the DatasetUsage will be copied.
     */
    public UsageToMap(Map<UUID,Long> usages){
      this.usages = usages;
    }

    @Override
    public boolean apply(
      @Nullable DatasetOccurrenceDownloadUsage input
    ) {
      return usages.put(input.getDatasetKey(), input.getNumberRecords()) == null;
    }
  }

  /**
   * Persists the dataset usage into the Registry data base.
   */
  public static class PersistUsage implements Predicate<DatasetOccurrenceDownloadUsage> {

    private final DatasetService datasetService;

    private final DatasetOccurrenceDownloadUsageService datasetUsageService;

    public PersistUsage(String registryWsUrl){
      RegistryClientUtil registryClientUtil = new RegistryClientUtil();
      datasetService = registryClientUtil.setupDatasetService(registryWsUrl);
      datasetUsageService = registryClientUtil.setupDatasetUsageService(registryWsUrl);
    }

    @Override
    public boolean apply(
      @Nullable DatasetOccurrenceDownloadUsage input
    ) {
      try {
        Dataset dataset = datasetService.get(input.getDatasetKey());
        if (dataset != null) { //the dataset still exists
          input.setDatasetDOI(dataset.getDoi());
          if (dataset.getCitation() != null && dataset.getCitation().getText() != null) {
            input.setDatasetCitation(dataset.getCitation().getText());
          }
          input.setDatasetTitle(dataset.getTitle());
          datasetUsageService.create(input);
        }
      } catch (Exception e) {
        LOG.error("Error persisting dataset usage information", e);
        return false;
      }
      return true;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CitationsFileReader.class);

  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();

  /**
   * Private constructor.
   */
  public CitationsFileReader() {

  }

  /**
   * Reads a dataset citations file with the form 'datasetkeyTABnumberOfRecords' and applies the listed predicates.
   * Each line in read from the TSV file is transformed into a DatasetOccurrenceDownloadUsage.
   * @param nameNode Hadoop name node uri
   * @param citationPath path to the directory that contains the citation table files
   * @param workflowId oozie workflow ID
   * @param predicates list of predicates to apply while reading the file
   * @throws IOException
   */
  public static void readCitations(String nameNode, String citationPath, String workflowId, Predicate<DatasetOccurrenceDownloadUsage>...predicates) throws IOException {
    final FileSystem hdfs = DownloadFileUtils.getHdfs(nameNode);
    for (FileStatus fs : hdfs.listStatus(new Path(citationPath))) {
      if (!fs.isDirectory()) {
        try(BufferedReader citationReader =
              new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath()), Charsets.UTF_8));
        ){
          for(String tsvLine = citationReader.readLine(); tsvLine != null; tsvLine = citationReader.readLine()) {
            if (!Strings.isNullOrEmpty(tsvLine)) {
              // catch all error to avoid breaking the loop
              try {
                for(Predicate<DatasetOccurrenceDownloadUsage> predicate : predicates){
                  predicate.apply(toDatasetOccurrenceDownloadUsage(tsvLine, DownloadUtils.workflowToDownloadId(workflowId)));
                }
              } catch (Throwable e) {
                LOG.info("Error processing citation line: {}", tsvLine);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Transforms tab-separated-line into a DatasetOccurrenceDownloadUsage instance.
   */
  private static DatasetOccurrenceDownloadUsage toDatasetOccurrenceDownloadUsage(String tsvLine, String downloadKey){
    final Iterator<String> tsvLineIterator = TAB_SPLITTER.split(tsvLine).iterator();
    DatasetOccurrenceDownloadUsage datasetUsage = new DatasetOccurrenceDownloadUsage();
    datasetUsage.setDatasetKey(UUID.fromString(tsvLineIterator.next()));
    datasetUsage.setDownloadKey(downloadKey);
    datasetUsage.setNumberRecords(Long.parseLong(tsvLineIterator.next()));
    return datasetUsage;
  }

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    readCitations(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY),
                  Preconditions.checkNotNull(args[0]),
                  Preconditions.checkNotNull(args[1]),
                  new PersistUsage(properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY)));
  }
}
