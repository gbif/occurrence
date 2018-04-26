package org.gbif.occurrence.download.file.simpleavro;

import com.google.common.base.Throwables;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.citations.CitationsFileReader;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.utils.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Combine the parts created by actor and combine them into single Avro file.
 */
public class SimpleAvroDownloadAggregator implements DownloadAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroDownloadAggregator.class);

  private static final String AVRO_EXTENSION = ".avro";

  private final DownloadJobConfiguration configuration;
  private final WorkflowConfiguration workflowConfiguration;
  private final String outputFileName;

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC_BY_4_0);
  private final CitationsFileReader.PersistUsage persistUsage;

  @Inject
  public SimpleAvroDownloadAggregator(
    DownloadJobConfiguration configuration,
    WorkflowConfiguration workflowConfiguration,
    OccurrenceDownloadService occurrenceDownloadService,
    DatasetService datasetService,
    DatasetOccurrenceDownloadUsageService occurrenceDownloadUsageService
  ) {
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
    outputFileName =
      configuration.getDownloadTempDir() + Path.SEPARATOR + configuration.getDownloadKey() + AVRO_EXTENSION;
    this.occurrenceDownloadService = occurrenceDownloadService;
    persistUsage = new CitationsFileReader.PersistUsage(datasetService, occurrenceDownloadUsageService);
  }

  public void init() {
    try {
      Files.createFile(Paths.get(outputFileName));
    } catch (Throwable t) {
      LOG.error("Error creating files", t);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    //init();
    try {
      if (!results.isEmpty()) {
        mergeResults(results);
      }
      SimpleAvroArchiveBuilder.mergeToSingleAvro(FileSystem.getLocal(new Configuration()).getRawFileSystem(),
        DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode()),
        configuration.getDownloadTempDir(),
        workflowConfiguration.getHdfsOutputPath(),
        configuration.getDownloadKey());
      //Delete the temp directory
      FileUtils.deleteDirectoryRecursively(Paths.get(configuration.getDownloadTempDir()).toFile());
    } catch (IOException ex) {
      LOG.error("Error aggregating download files", ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Merges the files of each job into a single CSV file.
   */
  private void mergeResults(List<Result> results) {

    ReflectDatumWriter<GenericContainer> rdw = new ReflectDatumWriter<>(GenericContainer.class);
    ReflectDatumReader<GenericContainer> rdr = new ReflectDatumReader<>(GenericContainer.class);
    boolean first = false;

    try (
      FileOutputStream outputFileWriter = new FileOutputStream(outputFileName, true);
      DataFileWriter<GenericContainer> dfw = new DataFileWriter(rdw);
    ) {
      // Results are sorted to respect the original ordering
      Collections.sort(results);
      DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
      for (Result result : results) {
        datasetUsagesCollector.sumUsages(result.getDatasetUsages());
        datasetUsagesCollector.mergeLicenses(result.getDatasetLicenses());

        InputStream is = new FileInputStream(result.getDownloadFileWork().getJobDataFileName());
        DataFileStream<GenericContainer> dfs = new DataFileStream(is, rdr);

        if (!first) {
          dfw.setCodec(CodecFactory.deflateCodec(-1));
          dfw.setFlushOnEveryBlock(false);
          dfw.create(dfs.getSchema(), outputFileWriter);
          first = true;
        }

        dfw.appendAllFrom(dfs, false);
      }
      dfw.flush();
      dfw.close();

      persistUsages(datasetUsagesCollector);
      persistDownloadLicense(configuration.getDownloadKey(), datasetUsagesCollector.getDatasetLicenses());
    } catch (Exception e) {
      LOG.error("Error merging results", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Persists the dataset usages collected in by the datasetUsagesCollector.
   */
  private void persistUsages(DatasetUsagesCollector datasetUsagesCollector) {
    for (Map.Entry<UUID, Long> usage : datasetUsagesCollector.getDatasetUsages().entrySet()) {
      DatasetOccurrenceDownloadUsage datasetOccurrenceDownloadUsage = new DatasetOccurrenceDownloadUsage();
      datasetOccurrenceDownloadUsage.setNumberRecords(usage.getValue());
      datasetOccurrenceDownloadUsage.setDatasetKey(usage.getKey());
      datasetOccurrenceDownloadUsage.setDownloadKey(configuration.getDownloadKey());
      persistUsage.apply(datasetOccurrenceDownloadUsage);
    }
  }

  /**
   * Persist download license that was assigned to the occurrence download.
   */
  private void persistDownloadLicense(String downloadKey, Set<License> licenses) {
    try {
      for (License license : licenses) {
        licenseSelector.collectLicense(license);
      }

      Download download = occurrenceDownloadService.get(configuration.getDownloadKey());
      download.setLicense(licenseSelector.getSelectedLicense());
      occurrenceDownloadService.update(download);
    } catch (Exception ex) {
      LOG.error("Error persisting download license information, downloadKey: {}, licenses:{} ", downloadKey, licenses,
        ex);
    }
  }
}
