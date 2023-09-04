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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.eml.DataDescription;
import org.gbif.api.vocabulary.ContactType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.Language;
import org.gbif.metadata.eml.EMLWriter;
import org.gbif.occurrence.query.HumanPredicateBuilder;
import org.gbif.occurrence.query.TitleLookupService;
import org.gbif.utils.file.FileUtils;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.File;
import java.io.Writer;
import java.net.URI;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.METADATA_FILENAME;

@Slf4j
public class DownloadMetadataBuilder implements Consumer<ConstituentDataset> {

  private static final String DOWNLOAD_CONTACT_SERVICE = "GBIF Download Service";
  private static final String DOWNLOAD_CONTACT_EMAIL = "support@gbif.org";

  private static final String DATASET_TITLE_FMT = "GBIF Occurrence Download %s";

  private static final String DATA_DESC_FORMAT = "Darwin Core Archive";

  private static final String METADATA_DESC_HEADER_FMT =
    "A dataset containing all occurrences available in GBIF matching the query:\n%s"
    +
    "\nThe dataset includes records from the following constituent datasets. "
    + "The full metadata for each constituent is also included in this archive:\n";

  private static final String RIGHTS =
    "The data included in this download are provided to the user under a %s license (%s), "
    + "please read the license terms and conditions to understand the implications of its usage and sharing."
    + "\nData from some individual datasets included in this download may be licensed under less restrictive terms; "
    + "review the details below.";

  private static final EMLWriter EML_WRITER = EMLWriter.newInstance(true);

  private static final ObjectMapper OBJECT_MAPPER = JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport();

  private final Download download;
  private final TitleLookupService titleLookup;
  private final File archiveDir;
  private final Function<Download,URI> downloadLinkProvider;
  private final StringBuilder description = new StringBuilder();

  @Builder
  public DownloadMetadataBuilder(
    Download download,
    TitleLookupService titleLookup,
    File archiveDir,
    Function<Download,URI> downloadLinkProvider
  ) {
    this.download = download;
    this.titleLookup = titleLookup;
    this.archiveDir = archiveDir;
    this.downloadLinkProvider = downloadLinkProvider;
    initDescription();
  }

  @SneakyThrows
  private String jsonPredicate() {
    if (download.getRequest() instanceof PredicateDownloadRequest) {
      return OBJECT_MAPPER.writeValueAsString(((PredicateDownloadRequest) download.getRequest()).getPredicate());
    }
    return "";
  }

  private String readPredicateQuery() {
    String humanQuery = jsonPredicate();
    try {
      if (download.getRequest() instanceof PredicateDownloadRequest) {
        Predicate p = ((PredicateDownloadRequest) download.getRequest()).getPredicate();
        return new HumanPredicateBuilder(titleLookup).humanFilterString(p);
      }
    } catch (Exception e) {
      log.error("Failed to transform JSON query into human query: {}", humanQuery, e);
    }
    return humanQuery;
  }

  private void initDescription() {
    // transform json filter into predicate instance and then into human-readable string
    String humanQuery = readPredicateQuery();
    description.append(String.format(METADATA_DESC_HEADER_FMT, humanQuery));
  }

  /**
   * Creates the dataset description.
   */
  @Override
  public void accept(ConstituentDataset constituentDataset) {
    description.append(constituentDataset.getRecords())
      .append(" records from ")
      .append(constituentDataset.getDataset().getTitle())
      .append('\n');
  }

  @SneakyThrows
  private DataDescription createDataDescription() {
    // link back to archive
    DataDescription dataDescription = new DataDescription();
    dataDescription.setName(DATA_DESC_FORMAT);
    dataDescription.setFormat(DATA_DESC_FORMAT);
    dataDescription.setCharset(Charsets.UTF_8.displayName());
    dataDescription.setUrl(downloadLinkProvider.apply(download));
    return dataDescription;
  }

  private Dataset derivedDatasetFromDownload(){
    Dataset dataset = new Dataset();
    // Random UUID use because the downloadKey is not a string in UUID format
    String downloadUniqueID = download.getKey();
    if (download.getDoi() != null) {
      downloadUniqueID = download.getDoi().getDoiName();
      dataset.setDoi(download.getDoi());
      Identifier identifier = new Identifier();
      identifier.setCreated(download.getCreated());
      identifier.setIdentifier(download.getKey());
      identifier.setType(IdentifierType.GBIF_PORTAL);
      dataset.setIdentifiers(Lists.newArrayList(identifier));
    }
    dataset.setKey(UUID.randomUUID());
    dataset.setTitle(String.format(DATASET_TITLE_FMT, downloadUniqueID));
    dataset.setDescription(description.toString());
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
    return dataset;
  }

  @SneakyThrows
  private void writeEmlMetadata(Dataset dataset) {
    File eml = new File(archiveDir, METADATA_FILENAME);
    try (Writer writer = FileUtils.startNewUtf8File(eml)) {
      EML_WRITER.writeTo(dataset, writer);
    }
  }

  public void writeMetadata() {
    log.info("Add query dataset metadata to archive");
    try {
      Dataset dataset = derivedDatasetFromDownload();
      writeEmlMetadata(dataset);
    } catch (Exception e) {
      log.error("Failed to write query result dataset EML file", e);
    }
  }

}
