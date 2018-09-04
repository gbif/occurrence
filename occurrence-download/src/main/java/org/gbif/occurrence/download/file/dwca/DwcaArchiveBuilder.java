package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Contact;
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
import org.gbif.occurrence.common.download.DownloadException;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.occurrence.download.util.HeadersFileUtil;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.occurrence.query.HumanFilterBuilder;
import org.gbif.occurrence.query.TitleLookup;
import org.gbif.occurrence.query.TitleLookupModule;
import org.gbif.registry.metadata.EMLWriter;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.CITATIONS_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.METADATA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.MULTIMEDIA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.RIGHTS_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.VERBATIM_FILENAME;

/**
 * Creates a dwc archive for occurrence downloads based on the hive query result files generated
 * during the Oozie workflow. It create a local archive folder with an occurrence data file and a dataset subfolder
 * that contains an EML metadata file per dataset involved.
 */
public class DwcaArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaArchiveBuilder.class);
  // The CRC is created by the function FileSystem.copyMerge function
  private static final String CRC_FILE_FMT = ".%s.crc";
  private static final String DOWNLOAD_CONTACT_SERVICE = "GBIF Download Service";
  private static final String DOWNLOAD_CONTACT_EMAIL = "support@gbif.org";
  private static final String METADATA_DESC_HEADER_FMT =
    "A dataset containing all occurrences available in GBIF matching the query:\n%s"
    +
    "\nThe dataset includes records from the following constituent datasets. "
    + "The full metadata for each constituent is also included in this archive:\n";
  private static final String CITATION_HEADER =
    "Please cite this data as follows, and pay attention to the rights documented in the rights.txt:\n"
    + "Please respect the rights declared for each dataset in the download: ";
  private static final String DATASET_TITLE_FMT = "GBIF Occurrence Download %s";
  private static final String RIGHTS =
    "The data included in this download are provided to the user under a %s license (%s), please read the license terms and conditions to understand the implications of its usage and sharing.\n\nData from some individual datasets included in this download may be licensed under less restrictive terms; review the details below.";
  private static final String DATA_DESC_FORMAT = "Darwin Core Archive";
  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();
  private static final EMLWriter EML_WRITER = EMLWriter.newInstance(true);

  private final DatasetService datasetService;

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final TitleLookup titleLookup;
  private final Dataset dataset;
  private final File archiveDir;
  private final WorkflowConfiguration workflowConfiguration;
  private final FileSystem sourceFs;
  private final FileSystem targetFs;
  private final DownloadJobConfiguration configuration;
  private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC_BY_4_0);
  private final List<Constituent> constituents = Lists.newArrayList();
  private final Ordering<Constituent> constituentsOrder =
    Ordering.natural().onResultOf(new Function<Constituent, Integer>() {

      public Integer apply(Constituent c) {
        return c.records;
      }
    });

  public static void buildArchive(DownloadJobConfiguration configuration, RegistryClientUtil registryClientUtil) throws IOException {
    buildArchive(configuration, new WorkflowConfiguration(), registryClientUtil);
  }

  public static void buildArchive(DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration, RegistryClientUtil registryClientUtil)
    throws IOException {
    String tmpDir = workflowConfiguration.getTempDir();

    // create temporary, local, download specific directory
    File archiveDir = new File(tmpDir, configuration.getDownloadKey());

    String registryWs = workflowConfiguration.getRegistryWsUrl();
    // create registry client and services
    DatasetService datasetService = registryClientUtil.setupDatasetService(registryWs);
    OccurrenceDownloadService occurrenceDownloadService = registryClientUtil.setupOccurrenceDownloadService(registryWs);

    Injector inj = Guice.createInjector(new TitleLookupModule(true, workflowConfiguration.getApiUrl()));
    TitleLookup titleLookup = inj.getInstance(TitleLookup.class);

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
    generator.buildArchive(new File(tmpDir, configuration.getDownloadKey() + ".zip"));
  }

  private static String writeCitation(Writer citationWriter, Dataset dataset, UUID constituentId)
    throws IOException {
    // citation
    String citationLink = null;
    if (dataset.getCitation() != null && !Strings.isNullOrEmpty(dataset.getCitation().getText())) {
      citationWriter.write('\n' + dataset.getCitation().getText());
      if (!Strings.isNullOrEmpty(dataset.getCitation().getIdentifier())) {
        citationLink = ", " + dataset.getCitation().getIdentifier();
        citationWriter.write(citationLink);
      }
    } else {
      LOG.error(String.format("Constituent dataset misses mandatory citation for id: %s", constituentId));
    }
    if (dataset.getDoi() != null) {
      citationWriter.write(" " + dataset.getDoi());
    }
    return citationLink;
  }

  /**
   * Write rights text.
   */
  private static void writeRights(Writer rightsWriter, Dataset dataset, String citationLink)
    throws IOException {
    // write rights
    rightsWriter.write("\n\nDataset: " + dataset.getTitle());
    if (!Strings.isNullOrEmpty(citationLink)) {
      rightsWriter.write(citationLink);
    }
    rightsWriter.write("\nRights as supplied: ");
    if (dataset.getLicense() != null && dataset.getLicense().isConcrete()) {
      rightsWriter.write(dataset.getLicense().getLicenseUrl());
    } else {
      rightsWriter.write("Not supplied");
    }
  }

  @VisibleForTesting
  protected DwcaArchiveBuilder(
    DatasetService datasetService,
    OccurrenceDownloadService occurrenceDownloadService,
    FileSystem sourceFs,
    FileSystem targetFs,
    File archiveDir,
    TitleLookup titleLookup,
    DownloadJobConfiguration configuration,
    WorkflowConfiguration workflowConfiguration
  ) {
    this.datasetService = datasetService;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.sourceFs = sourceFs;
    this.targetFs = targetFs;
    this.archiveDir = archiveDir;
    this.titleLookup = titleLookup;
    dataset = new Dataset();
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
  }

  /**
   * Main method to assemble the dwc archive and do all the work until we have a final zip file.
   *
   * @param zipFile the final zip file holding the entire archive
   */
  public void buildArchive(File zipFile) throws DownloadException {
    LOG.info("Start building the archive {} ", zipFile.getPath());

    try {
      if (zipFile.exists()) {
        zipFile.delete();
      }
      if (!configuration.isSmallDownload()) {
        // oozie might try several times to run this job, so make sure our filesystem is clean
        cleanupFS();

        // create the temp archive dir
        archiveDir.mkdirs();
      }

      // metadata, citation and rights
      License downloadLicense = addConstituentMetadata();

      // persist the License assigned to the download
      persistDownloadLicense(configuration.getDownloadKey(), downloadLicense);

      // metadata about the entire archive data
      generateMetadata();

      // meta.xml
      DwcArchiveUtils.createArchiveDescriptor(archiveDir);

      // zip up
      LOG.info("Zipping archive {}", archiveDir.toString());
      CompressionUtil.zipDir(archiveDir, zipFile, true);

      // add the large download data files to the zip stream
      if (!configuration.isSmallDownload()) {
        appendPreCompressedFiles(zipFile);
      }
      targetFs.moveFromLocalFile(new Path(zipFile.getPath()),
                                 new Path(workflowConfiguration.getHdfsOutputPath(), zipFile.getName()));

    } catch (IOException e) {
      throw new DownloadException(e);

    } finally {
      // always cleanUp temp dir
      cleanupFS();
    }

  }

  public void createEmlFile(UUID constituentId, File emlDir) throws IOException {
    Closer closer = Closer.create();
    try {
      // store dataset EML as constituent metadata
      InputStream in = closer.register(datasetService.getMetadataDocument(constituentId));
      if (in != null) {
        // copy into archive, reading stream from registry services
        OutputStream out = closer.register(new FileOutputStream(new File(emlDir, constituentId + ".xml")));
        ByteStreams.copy(in, out);
      } else {
        LOG.error("Found no EML for datasetId {}", constituentId);
      }

    } catch (FileNotFoundException ex) {
      LOG.error("Error creating eml file", ex);
    } catch (IOException ex) {
      LOG.error("Error creating eml file", ex);
    } finally {
      closer.close();
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
      humanQuery = new HumanFilterBuilder(titleLookup).humanFilterString(p);
    } catch (Exception e) {
      LOG.error("Failed to transform JSON query into human query: {}", configuration.getFilter(), e);
    }

    description.append(String.format(METADATA_DESC_HEADER_FMT, humanQuery));
    List<Constituent> byRecords = constituentsOrder.sortedCopy(constituents);
    for (Constituent c : byRecords) {
      description.append(c.records + " records from " + c.title + '\n');
    }
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
      LOG.error(String.format("Wrong url %s", workflowConfiguration.getDownloadLink(configuration.getDownloadKey())),
                e);
    }
    return dataDescription;
  }

  /**
   * Rewrites the zip file by opening the original and appending the pre-compressed content on the fly.
   */
  private void appendPreCompressedFiles(File zipFile) throws IOException {

    LOG.info("Appending pre-compressed occurrence content to the Zip: " + zipFile.getAbsolutePath());

    File tempZip = new File(archiveDir, zipFile.getName() + ".part");
    boolean renameOk = zipFile.renameTo(tempZip);
    if (renameOk) {
      try (
        ZipInputStream zin = new ZipInputStream(new FileInputStream(tempZip));
        ModalZipOutputStream out = new ModalZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFile)));
      ) {

        // copy existing entries
        ZipEntry entry = zin.getNextEntry();
        while (entry != null) {
          out.putNextEntry(new org.gbif.hadoop.compress.d2.zip.ZipEntry(entry.getName()),
                           ModalZipOutputStream.MODE.DEFAULT);
          ByteStreams.copy(zin, out);
          entry = zin.getNextEntry();
        }

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

      } finally {
        // we've rewritten so remove the original
        if (tempZip != null) {
          tempZip.delete();
        }
      }

    } else {
      throw new IllegalStateException("Unable to rename existing zip, to allow appending occurrence data");
    }
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
        LOG.info("Deflated content to merge: " + path);
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

    Map<UUID, Long> srcDatasets = readDatasetCounts(citationSrc);

    File emlDir = new File(archiveDir, "dataset");
    if (!srcDatasets.isEmpty()) {
      emlDir.mkdir();
    }
    Closer closer = Closer.create();

    Writer rightsWriter = closer.register(FileUtils.startNewUtf8File(new File(archiveDir, RIGHTS_FILENAME)));
    Writer citationWriter = closer.register(FileUtils.startNewUtf8File(new File(archiveDir, CITATIONS_FILENAME)));

    closer.register(citationWriter);
    // write fixed citations header
    citationWriter.write(CITATION_HEADER);
    // now iterate over constituent UUIDs

    for (Entry<UUID, Long> dsEntry : srcDatasets.entrySet()) {
      UUID constituentId = dsEntry.getKey();
      LOG.info("Processing constituent dataset: {}", constituentId);
      // catch errors for each uuid to make sure one broken dataset does not bring down the entire process
      try {
        Dataset srcDataset = datasetService.get(constituentId);

        licenseSelector.collectLicense(srcDataset.getLicense());
        // citation
        String citationLink = writeCitation(citationWriter, srcDataset, constituentId);
        // rights
        writeRights(rightsWriter, srcDataset, citationLink);
        // eml file
        createEmlFile(constituentId, emlDir);

        // add as constituent for later
        constituents.add(new Constituent(srcDataset.getTitle(), dsEntry.getValue().intValue()));

        // add original author as content provider to main dataset description
        Contact provider = DwcaContactsUtil.getContentProviderContact(srcDataset);
        if (provider != null) {
          dataset.getContacts().add(provider);
        }
      } catch (UniformInterfaceException e) {
        LOG.error(String.format("Registry client http exception: %d \n %s",
                                e.getResponse().getStatus(),
                                e.getResponse().getEntity(String.class)), e);
      } catch (Exception e) {
        LOG.error("Error creating download file", e);
        return licenseSelector.getSelectedLicense();
      }
    }
    closer.close();
    return licenseSelector.getSelectedLicense();
  }

  /**
   * Creates a single EML metadata file for the entire archive.
   * Make sure we execute this method AFTER building the constituents metadata which adds to our dataset instance.
   */
  private void generateMetadata() {
    LOG.info("Add query dataset metadata to archive");
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
      Citation citation = new Citation(String.format(DATASET_TITLE_FMT, downloadUniqueID), downloadUniqueID);
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
   * @param downloadKey
   * @param license
   */
  private void persistDownloadLicense(String downloadKey, License license) {
    try {
      Download download = occurrenceDownloadService.get(configuration.getDownloadKey());
      download.setLicense(license);
      occurrenceDownloadService.update(download);
    } catch (Exception ex) {
      LOG.error("Error updating download license, downloadKey: {}, license: {}", downloadKey, license, ex);
    }
  }

  /**
   * Creates Map with dataset UUIDs and its record counts.
   */
  private Map<UUID, Long> readDatasetCounts(Path citationSrc) throws IOException {
    // the hive query result is a directory with one or more files - read them all into a uuid set
    Map<UUID, Long> srcDatasets = Maps.newHashMap(); // map of uuids to occurrence counts
    FileStatus[] citFiles = sourceFs.listStatus(citationSrc);
    int invalidUuids = 0;
    Closer closer = Closer.create();
    for (FileStatus fs : citFiles) {
      if (!fs.isDirectory()) {
        BufferedReader citationReader =
          new BufferedReader(new InputStreamReader(sourceFs.open(fs.getPath()), Charsets.UTF_8));
        closer.register(citationReader);
        try {
          String line = citationReader.readLine();
          while (line != null) {
            if (!Strings.isNullOrEmpty(line)) {
              // we also catch errors for every dataset so we dont break the loop
              try {
                Iterator<String> iter = TAB_SPLITTER.split(line).iterator();
                // play safe and make sure we got a uuid - even though our api doesnt require it
                UUID key = UUID.fromString(iter.next());
                Long count = Long.parseLong(iter.next());
                srcDatasets.put(key, count);
               
              } catch (IllegalArgumentException e) {
                // ignore invalid UUIDs
                LOG.info("Found invalid UUID as datasetId {}", line);
                invalidUuids++;
              }
            }
            line = citationReader.readLine();
          }
        } finally {
          closer.close();
        }
      }
    }
    if (invalidUuids > 0) {
      LOG.info("Found {} invalid dataset UUIDs", invalidUuids);
    } else {
      LOG.info("All {} dataset UUIDs are valid", srcDatasets.size());
    }
      // small downloads persist dataset usages while builds the citations file
    if (!configuration.isSmallDownload()) {
      try {
        occurrenceDownloadService.createUsages(configuration.getDownloadKey(), srcDatasets);
      }
      catch(Exception e) {
        LOG.error("Error persisting dataset usage information, downloadKey: {} for large download", configuration.getDownloadKey(), e);
      }
    }
    return srcDatasets;
  }

  /**
   * Simple, local representation for a constituent dataset.
   */
  static class Constituent {

    private final String title;
    private final int records;

    Constituent(String title, int records) {
      this.title = title;
      this.records = records;
    }
  }
}
