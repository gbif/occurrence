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
package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.eml.DataDescription;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.ContactType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.Language;
import org.gbif.api.vocabulary.License;
import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.common.download.DownloadException;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.occurrence.download.util.HeadersFileUtil;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.occurrence.query.HumanPredicateBuilder;
import org.gbif.occurrence.query.TitleLookupService;
import org.gbif.occurrence.query.TitleLookupServiceFactory;
import org.gbif.registry.metadata.EMLWriter;
import org.gbif.utils.file.FileUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.CITATIONS_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.METADATA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.MULTIMEDIA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.RIGHTS_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.VERBATIM_FILENAME;

/**
 * Creates a DWC archive for occurrence downloads based on the hive query result files generated
 * during the Oozie workflow. It create a local archive folder with an occurrence data file and a dataset subfolder
 * that contains an EML metadata file per dataset involved.
 */
public class DwcaArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaArchiveBuilder.class);
  // The CRC is created by the function FileSystem.copyMerge function
  private static final String DOWNLOAD_CONTACT_SERVICE = "GBIF Download Service";
  private static final String DOWNLOAD_CONTACT_EMAIL = "support@gbif.org";
  private static final String METADATA_DESC_HEADER_FMT =
    "A dataset containing all occurrences available in GBIF matching the query:\n%s"
    +
    "\nThe dataset includes records from the following constituent datasets. "
    + "The full metadata for each constituent is also included in this archive:\n";
  private static final String CITATION_HEADER =
    "When using this dataset please use the following citation and pay attention to the rights documented in rights.txt:\n";
  private static final String DATASET_TITLE_FMT = "GBIF Occurrence Download %s";
  private static final String RIGHTS =
    "The data included in this download are provided to the user under a %s license (%s), please read the license terms and conditions to understand the implications of its usage and sharing.\nData from some individual datasets included in this download may be licensed under less restrictive terms; review the details below.";
  private static final String DATA_DESC_FORMAT = "Darwin Core Archive";
  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();
  private static final EMLWriter EML_WRITER = EMLWriter.newInstance(true);

  private final DatasetService datasetService;

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final TitleLookupService titleLookup;
  private final File archiveDir;
  private final WorkflowConfiguration workflowConfiguration;
  private final FileSystem sourceFs;
  private final FileSystem targetFs;
  private final DownloadJobConfiguration configuration;
  private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);

  //constituents and citation are basically the same info, are keep in 2 separate collections to avoid rebuilding them
  private Set<Constituent> constituents = Sets.newTreeSet();
  private Map<UUID,Long> citations = Maps.newHashMap();

  public static void buildArchive(DownloadJobConfiguration configuration) throws IOException {
    buildArchive(configuration, new WorkflowConfiguration());
  }

  public static void buildArchive(DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration)
    throws IOException {
    RegistryClientUtil registryClientUtil = new RegistryClientUtil(workflowConfiguration.getRegistryWsUrl());
    String tmpDir = workflowConfiguration.getTempDir();

    // create temporary, local, download specific directory
    File archiveDir = new File(tmpDir, configuration.getDownloadKey());

    // create registry client and services
    DatasetService datasetService = registryClientUtil.setupDatasetService();
    OccurrenceDownloadService occurrenceDownloadService = registryClientUtil.setupOccurrenceDownloadService(configuration.getCoreTerm());
    TitleLookupService titleLookup = TitleLookupServiceFactory.getInstance(workflowConfiguration.getApiUrl());

    FileSystem sourceFs = configuration.isSmallDownload()
      ? FileSystem.getLocal(workflowConfiguration.getHadoopConf())
      : FileSystem.get(workflowConfiguration.getHadoopConf());
    FileSystem targetFs = FileSystem.get(workflowConfiguration.getHadoopConf());

    // build archive
    DwcaArchiveBuilder generator = new DwcaArchiveBuilder(datasetService,
                                                          occurrenceDownloadService,
                                                          sourceFs,
                                                          targetFs,
                                                          archiveDir,
                                                          titleLookup,
                                                          configuration,
                                                          workflowConfiguration);
    generator.buildArchive();
  }

  private static void writeCitation(Writer citationWriter, Dataset dataset)
    throws IOException {
    // citation
    if (dataset.getCitation() != null && !Strings.isNullOrEmpty(dataset.getCitation().getText())) {
      citationWriter.write(dataset.getCitation().getText());
      citationWriter.write('\n');
    } else {
      LOG.error("Constituent dataset misses mandatory citation for id: {}", dataset.getKey());
    }
  }

  /**
   * Write rights text.
   */
  private static void writeRights(Writer rightsWriter, Dataset dataset)
    throws IOException {
    // write rights
    rightsWriter.write("\nDataset: " + dataset.getTitle());

    rightsWriter.write("\nRights as supplied: ");
    if (dataset.getLicense() != null && dataset.getLicense().isConcrete()) {
      rightsWriter.write(dataset.getLicense().getLicenseUrl());
    } else {
      rightsWriter.write("Not supplied");
    }
  }

  @VisibleForTesting
  protected DwcaArchiveBuilder(DatasetService datasetService, OccurrenceDownloadService occurrenceDownloadService,
                               FileSystem sourceFs, FileSystem targetFs, File archiveDir, TitleLookupService titleLookup,
                               DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration) {
    this.datasetService = datasetService;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.sourceFs = sourceFs;
    this.targetFs = targetFs;
    this.archiveDir = archiveDir;
    this.titleLookup = titleLookup;
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
  }

  /**
   * Main method to assemble the DwC archive and do all the work until we have a final zip file.
   */
  public void buildArchive() throws DownloadException {
    LOG.info("Start building the archive for {} ", configuration.getDownloadKey());

    String zipFileName = configuration.getDownloadKey() + ".zip";

    try {
      if (!configuration.isSmallDownload()) {
        // oozie might try several times to run this job, so make sure our filesystem is clean
        cleanupFS();

        // create the temp archive dir
        archiveDir.mkdirs();
      }

      // metadata, citation and rights
      License downloadLicense = addConstituentMetadata();

      // persist the License assigned to the download
      persistDownloadLicense(downloadLicense);

      // metadata about the entire archive data
      generateMetadata();

      // meta.xml
      DwcArchiveUtils.createArchiveDescriptor(archiveDir);

      // zip up
      Path hdfsTmpZipPath = new Path(workflowConfiguration.getHdfsTempDir(), zipFileName);
      LOG.info("Zipping archive {} to HDFS temporary location {}", archiveDir, hdfsTmpZipPath);

      try (
        FSDataOutputStream zipped = targetFs.create(hdfsTmpZipPath, true);
        ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped, 10*1024*1024))
      ) {
        zipLocalFiles(zos);

        // add the large download data files to the zip stream
        if (!configuration.isSmallDownload()) {
          appendPreCompressedFiles(zos);
        }

        zos.finish();
      }

      LOG.info("Moving Zip from HDFS temporary location to final destination.");
      targetFs.rename(hdfsTmpZipPath,
        new Path(workflowConfiguration.getHdfsOutputPath(), zipFileName));

    } catch (IOException e) {
      throw new DownloadException(e);

    } finally {
      // always cleanUp temp dir
      cleanupFS();
    }

  }

  /**
   * Merges the file using the standard java libraries java.util.zip.
   */
  private void zipLocalFiles(ModalZipOutputStream zos) {
    try {
      Collection<File> files = org.apache.commons.io.FileUtils.listFiles(archiveDir, null, true);

      for (File f : files) {
        LOG.debug("Adding local file {} to archive", f);
        try (FileInputStream fileInZipInputStream = new FileInputStream(f)) {
          String zipPath = StringUtils.removeStart(f.getAbsolutePath(), archiveDir.getAbsolutePath() + File.separator);
          ZipEntry entry = new ZipEntry(zipPath);
          zos.putNextEntry(entry, ModalZipOutputStream.MODE.DEFAULT);
          ByteStreams.copy(fileInZipInputStream, zos);
        }
      }
      zos.closeEntry();

    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  public void createEmlFile(UUID constituentId, File emlDir) {
    try (InputStream in = datasetService.getMetadataDocument(constituentId)) {
      // store dataset EML as constituent metadata
      if (in != null) {
        // copy into archive, reading stream from registry services
        try(OutputStream out = new FileOutputStream(new File(emlDir, constituentId + ".xml"))) {
          ByteStreams.copy(in, out);
        }
      } else {
        LOG.error("Found no EML for datasetId {}", constituentId);
      }
    } catch (IOException ex) {
      LOG.error("Error creating eml file", ex);
    }
  }

  /**
   * Creates the dataset description.
   */
  @VisibleForTesting
  protected String getDatasetDescription() {
    StringBuilder description = new StringBuilder();
    // transform json filter into predicate instance and then into human readable string
    String humanQuery = configuration.getFilter();
    try {
      ObjectMapper mapper = new ObjectMapper();
      Predicate p = mapper.readValue(configuration.getFilter(), Predicate.class);
      humanQuery = new HumanPredicateBuilder(titleLookup).humanFilterString(p);
    } catch (Exception e) {
      LOG.error("Failed to transform JSON query into human query: {}", configuration.getFilter(), e);
    }

    description.append(String.format(METADATA_DESC_HEADER_FMT, humanQuery));
    constituents.stream()
      .map(c -> c.getRecords() + " records from " + c.getDataset().getTitle() + '\n')
      .forEach(description::append);
    return description.toString();
  }

  protected DataDescription createDataDescription() {
    // link back to archive
    DataDescription dataDescription = new DataDescription();
    dataDescription.setName(DATA_DESC_FORMAT);
    dataDescription.setFormat(DATA_DESC_FORMAT);
    dataDescription.setCharset(Charsets.UTF_8.displayName());
    try {
      dataDescription.setUrl(new URI(workflowConfiguration.getDownloadLink(configuration.getDownloadKey())));
    } catch (URISyntaxException e) {
      LOG.error("Wrong url {}", workflowConfiguration.getDownloadLink(configuration.getDownloadKey()), e);
    }
    return dataDescription;
  }

  /**
   * Append the pre-compressed content to the zip stream
   */
  private void appendPreCompressedFiles(ModalZipOutputStream out) throws IOException {
    LOG.info("Appending pre-compressed occurrence content to the Zip");

    // NOTE: hive lowercases all the paths
    appendPreCompressedFile(out,
      new Path(configuration.getInterpretedDataFileName()),
      INTERPRETED_FILENAME,
      HeadersFileUtil.getInterpretedTableHeader());
    appendPreCompressedFile(out,
      new Path(configuration.getVerbatimDataFileName()),
      VERBATIM_FILENAME,
      HeadersFileUtil.getVerbatimTableHeader());
    appendPreCompressedFile(out,
      new Path(configuration.getMultimediaDataFileName()),
      MULTIMEDIA_FILENAME,
      HeadersFileUtil.getMultimediaTableHeader());
  }

  /**
   * Appends the compressed files found within the directory to the zip stream as the named file
   */
  private void appendPreCompressedFile(ModalZipOutputStream out, Path dir, String filename, String headerRow)
    throws IOException {
    RemoteIterator<LocatedFileStatus> files = sourceFs.listFiles(dir, false);
    List<InputStream> parts = Lists.newArrayList();

    // Add the header first, which must also be compressed
    ByteArrayOutputStream header = new ByteArrayOutputStream();
    D2Utils.compress(new ByteArrayInputStream(headerRow.getBytes()), header);
    parts.add(new ByteArrayInputStream(header.toByteArray()));

    // Locate the streams to the compressed content on HDFS
    while (files.hasNext()) {
      LocatedFileStatus fs = files.next();
      Path path = fs.getPath();
      if (path.toString().endsWith(D2Utils.FILE_EXTENSION)) {
        LOG.info("Deflated content to merge: {} ", path);
        parts.add(sourceFs.open(path));
      }
    }

    // create the Zip entry, and write the compressed bytes
    org.gbif.hadoop.compress.d2.zip.ZipEntry ze = new org.gbif.hadoop.compress.d2.zip.ZipEntry(filename);
    out.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED);
    try (D2CombineInputStream in = new D2CombineInputStream(parts)) {
      ByteStreams.copy(in, out);
      in.close(); // important so counts are accurate
      ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
      ze.setCompressedSize(in.getCompressedLength());
      ze.setCrc(in.getCrc32());
    } finally {
      out.closeEntry();
    }
  }

  /**
   * Adds an eml file per dataset involved into a subfolder "dataset" which is supported by our dwc archive reader.
   * Create a rights.txt and citation.txt file targeted at humans to quickly yield an overview about rights and
   * datasets involved.
   * This method returns the License that must be assigned to the occurrence download file.
   */
  private License addConstituentMetadata() throws IOException {

    Path citationSrc = new Path(configuration.getCitationDataFileName());

    LOG.info("Adding constituent dataset metadata to archive, based on: {}", citationSrc);

    // now read the dataset citation table and create an EML file per datasetId
    // first copy from HDFS to local file
    if (!sourceFs.exists(citationSrc)) {
      LOG.warn("No citation file directory existing on HDFS, skip creating of dataset metadata {}", citationSrc);
      return licenseSelector.getSelectedLicense();
    }

    constituents = loadCitations(citationSrc);

    File emlDir = new File(archiveDir, "dataset");
    if (!constituents.isEmpty()) {
      emlDir.mkdir();
    }

    try(Writer rightsWriter = FileUtils.startNewUtf8File(new File(archiveDir, RIGHTS_FILENAME));
        Writer citationWriter = FileUtils.startNewUtf8File(new File(archiveDir, CITATIONS_FILENAME))) {
        // write fixed citations header
        citationWriter.write(CITATION_HEADER);
        // now iterate over constituent UUIDs

        for (Constituent constituent : constituents) {
          LOG.info("Processing constituent dataset: {}", constituent.getKey());
          // catch errors for each uuid to make sure one broken dataset does not bring down the entire process
          try {
            Dataset dataset = constituent.getDataset();

            licenseSelector.collectLicense(constituent.getDataset().getLicense());
            // citation
            writeCitation(citationWriter, constituent.getDataset());
            // rights
            writeRights(rightsWriter, constituent.getDataset());
            // eml file
            createEmlFile(dataset.getKey(), emlDir);

            // add original author as content provider to main dataset description
            DwcaContactsUtil.getContentProviderContact(constituent.getDataset())
              .ifPresent(provider -> dataset.getContacts().add(provider));
          } catch (Exception e) {
            LOG.error("Error creating download file", e);
            return licenseSelector.getSelectedLicense();
          }
        }
    }
    return licenseSelector.getSelectedLicense();
  }

  /**
   * Creates a single EML metadata file for the entire archive.
   * Make sure we execute this method AFTER building the constituents metadata which adds to our dataset instance.
   */
  private void generateMetadata() {
    LOG.info("Add query dataset metadata to archive");
    Dataset dataset = new Dataset();
    try {
      // Random UUID use because the downloadKey is not a string in UUID format
      Download download = occurrenceDownloadService.get(configuration.getDownloadKey());
      String downloadUniqueID = configuration.getDownloadKey();
      if (download.getDoi() != null) {
        downloadUniqueID = download.getDoi().getDoiName();
        dataset.setDoi(download.getDoi());
        Identifier identifier = new Identifier();
        identifier.setCreated(download.getCreated());
        identifier.setIdentifier(configuration.getDownloadKey());
        identifier.setType(IdentifierType.GBIF_PORTAL);
        dataset.setIdentifiers(Lists.newArrayList(identifier));
      }
      dataset.setKey(UUID.randomUUID());
      dataset.setTitle(String.format(DATASET_TITLE_FMT, downloadUniqueID));
      dataset.setDescription(getDatasetDescription());
      dataset.setCreated(download.getCreated());
      Citation citation = new Citation(String.format(DATASET_TITLE_FMT, downloadUniqueID), downloadUniqueID, false);
      dataset.setCitation(citation);
      // can we derive a link from the query to set the dataset.homepage?
      dataset.setPubDate(download.getCreated());
      dataset.setDataLanguage(Language.ENGLISH);
      dataset.setType(DatasetType.OCCURRENCE);
      dataset.getDataDescriptions().add(createDataDescription());
      //TODO: use new license field once available
      if (download.getLicense().isConcrete()) {
        dataset.setRights(String.format(RIGHTS, download.getLicense().getLicenseTitle(), download.getLicense().getLicenseUrl()));
      }
      dataset.getContacts()
        .add(DwcaContactsUtil.createContact(DOWNLOAD_CONTACT_SERVICE,
                                            DOWNLOAD_CONTACT_EMAIL,
                                            ContactType.ORIGINATOR,
                                            true));
      dataset.getContacts()
        .add(DwcaContactsUtil.createContact(DOWNLOAD_CONTACT_SERVICE,
                                            DOWNLOAD_CONTACT_EMAIL,
                                            ContactType.ADMINISTRATIVE_POINT_OF_CONTACT,
                                            true));
      dataset.getContacts()
        .add(DwcaContactsUtil.createContact(DOWNLOAD_CONTACT_SERVICE,
                                            DOWNLOAD_CONTACT_EMAIL,
                                            ContactType.METADATA_AUTHOR,
                                            true));

      File eml = new File(archiveDir, METADATA_FILENAME);
      Writer writer = FileUtils.startNewUtf8File(eml);
      EML_WRITER.writeTo(dataset, writer);

    } catch (Exception e) {
      LOG.error("Failed to write query result dataset EML file", e);
    }
  }

  /**
   * Removes all temporary file system artifacts but the final zip archive.
   */
  private void cleanupFS() throws DownloadException {
    LOG.info("Cleaning up archive directory {}", archiveDir.getPath());
    if (archiveDir.exists()) {
      FileUtils.deleteDirectoryRecursively(archiveDir);
    }
  }

  /**
   * Persist download license that was assigned to the occurrence download.
   *
   * @param license
   */
  private void persistDownloadLicense(License license) {
    try {
      Download download = occurrenceDownloadService.get(configuration.getDownloadKey());
      download.setLicense(license);
      occurrenceDownloadService.update(download);
    } catch (Exception ex) {
      LOG.error("Error updating download license, downloadKey: {}, license: {}", configuration.getDownloadKey(), license, ex);
    }
  }

  /**
   * Creates Map with dataset UUIDs and its record counts.
   */
  private Set<Constituent> loadCitations(Path citationSrc) throws IOException {
    // the hive query result is a directory with one or more files - read them all into a uuid set
    Set<Constituent> datasets = Sets.newTreeSet(); // list of constituents datasets
    FileStatus[] citFiles = sourceFs.listStatus(citationSrc);
    int invalidUuids = 0;
    for (FileStatus fs : citFiles) {
      if (!fs.isDirectory()) {
        try (BufferedReader citationReader =
               new BufferedReader(new InputStreamReader(sourceFs.open(fs.getPath()), Charsets.UTF_8))) {

          String line = citationReader.readLine();
          while (line != null) {
            if (!Strings.isNullOrEmpty(line)) {
              // we also catch errors for every dataset so we dont break the loop
              try {
                Iterator<String> iter = TAB_SPLITTER.split(line).iterator();
                // play safe and make sure we got a uuid - even though our api doesnt require it
                UUID key = UUID.fromString(iter.next());
                long count = Long.parseLong(iter.next());
                datasets.add(new Constituent(key, count, datasetService.get(key)));
                citations.put(key, count);
              } catch (Exception e) {
                // ignore invalid UUIDs
                LOG.info("Found invalid UUID as datasetId {}", line);
                invalidUuids++;
              }
            }
            line = citationReader.readLine();
          }
        }
      }
    }
    if (invalidUuids > 0) {
      LOG.info("Found {} invalid dataset UUIDs", invalidUuids);
    } else {
      LOG.info("All {} dataset UUIDs are valid", datasets.size());
    }
      // small downloads persist dataset usages while builds the citations file
    if (!configuration.isSmallDownload()) {
      try {
        occurrenceDownloadService.createUsages(configuration.getDownloadKey(), citations);
      }
      catch(Exception e) {
        LOG.error("Error persisting dataset usage information, downloadKey: {} for large download", configuration.getDownloadKey(), e);
      }
    }
    return datasets;
  }

  /**
   * Simple, local representation for a constituent dataset.
   */
  static class Constituent implements  Comparable<Constituent> {

    //Comparator based on number of records and then key
    private static final Comparator<Constituent> CONSTITUENT_COMPARATOR = Comparator.comparingLong(Constituent::getRecords).thenComparing(Constituent::getKey);

    private final UUID key;
    private final long records;
    private final Dataset dataset;

    Constituent(UUID key, long records, Dataset dataset) {
      this.key = key;
      this.records = records;
      this.dataset = dataset;
    }

    public UUID getKey() {
      return key;
    }

    public long getRecords() {
      return records;
    }

    public Dataset getDataset() {
      return dataset;
    }

    @Override
    public int compareTo(Constituent other) {
      return CONSTITUENT_COMPARATOR.compare(this, other);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
              .add("key", key)
              .add("records", records)
              .add("dataset", dataset).toString();
    }
  }

}
