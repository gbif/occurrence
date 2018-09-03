package org.gbif.occurrence.download.citations;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
 * Reads a datasets citations file and optionally persists the data usages and return the usages into a Map object.
 */
public final class CitationsFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(CitationsFileReader.class);
  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();

  /**
   * Transforms tab-separated-line into a DatasetOccurrenceDownloadUsage instance.
   */
  private static DatasetOccurrenceDownloadUsage toDatasetOccurrenceDownloadUsage(String tsvLine, String downloadKey) {
    Iterator<String> tsvLineIterator = TAB_SPLITTER.split(tsvLine).iterator();
    DatasetOccurrenceDownloadUsage datasetUsage = new DatasetOccurrenceDownloadUsage();
    datasetUsage.setDatasetKey(UUID.fromString(tsvLineIterator.next()));
    datasetUsage.setDownloadKey(downloadKey);
    datasetUsage.setNumberRecords(Long.parseLong(tsvLineIterator.next()));
    return datasetUsage;
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
  public static void readCitations(String nameNode, String citationPath, String downloadKey,
                                   Predicate<List<DatasetOccurrenceDownloadUsage>> predicate) throws IOException {
    List<DatasetOccurrenceDownloadUsage> citationsList = new LinkedList<>();
    FileSystem hdfs = DownloadFileUtils.getHdfs(nameNode);
    for (FileStatus fs : hdfs.listStatus(new Path(citationPath))) {
      if (!fs.isDirectory()) {
        try (BufferedReader citationReader = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath()),
                                                                                      Charsets.UTF_8))) {
          for (String tsvLine = citationReader.readLine(); tsvLine != null; tsvLine = citationReader.readLine()) {
            if (!Strings.isNullOrEmpty(tsvLine)) {
              // prepare citation object and add it to list
                  citationsList.add(toDatasetOccurrenceDownloadUsage(tsvLine, downloadKey));
             
            }
          }
        }
      }
    }
    predicate.apply(citationsList);
  }

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    readCitations(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY),
                  Preconditions.checkNotNull(args[0]),
                  Preconditions.checkNotNull(args[1]),
                  new PersistUsage(properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY)));
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
  public static class PersistUsage implements Predicate<List<DatasetOccurrenceDownloadUsage>> {

    private final DatasetService datasetService;

    private final DatasetOccurrenceDownloadUsageService datasetUsageService;

    public PersistUsage(String registryWsUrl) {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil();
      datasetService = registryClientUtil.setupDatasetService(registryWsUrl);
      datasetUsageService = registryClientUtil.setupDatasetUsageService(registryWsUrl);
    }

    public PersistUsage(DatasetService datasetService, DatasetOccurrenceDownloadUsageService datasetUsageService) {
      this.datasetService = datasetService;
      this.datasetUsageService = datasetUsageService;
    }

    @Override
    public boolean apply(@Nullable List<DatasetOccurrenceDownloadUsage> inputs) {
      if(inputs==null || inputs.isEmpty()) {
        LOG.info("No citation information to update as list of datasets is empty or null, hence ignoring the request");
        return true;
      }
      //enriching the list with doi,citation and title.
      for(int i=0;i<inputs.size();i++) {
        DatasetOccurrenceDownloadUsage input=inputs.get(i);
        Dataset dataset = datasetService.get(input.getDatasetKey());
        if (dataset != null) { //the dataset still exists
          input.setDatasetDOI(dataset.getDoi());
          if (dataset.getCitation() != null && dataset.getCitation().getText() != null) {
            input.setDatasetCitation(dataset.getCitation().getText());
          }
          input.setDatasetTitle(dataset.getTitle());
          inputs.set(i, input);
        }
      }
      try {
        datasetUsageService.bulkCreate(inputs);
      } catch (Exception e) {
        LOG.error("Error persisting dataset usage information {}", inputs, e);
        return false;
      }
      return true;
    }
  }
}
