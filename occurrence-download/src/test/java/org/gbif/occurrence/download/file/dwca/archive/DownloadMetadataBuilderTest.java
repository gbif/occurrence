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

import org.gbif.api.model.common.DOI;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.query.TitleLookupService;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class DownloadMetadataBuilderTest {

  @TempDir
  static Path tempDir;

  @SneakyThrows
  private Predicate testPredicate() {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue("{\n"
                                  + "    \"type\":\"and\",\n"
                                  + "    \"predicates\":[\n"
                                  + "      {\n"
                                  + "        \"type\":\"equals\",\n"
                                  + "        \"key\":\"DATASET_KEY\",\n"
                                  + "        \"value\":\"6bff1c15-43a6-4074-8b9c-3994cc8fe028\"\n"
                                  + "      },\n"
                                  + "      {\n"
                                  + "        \"type\":\"equals\",\n"
                                  + "        \"key\":\"DWCA_EXTENSION\",\n"
                                  + "        \"value\":\"http://rs.gbif.org/terms/1.0/Multimedia\"\n"
                                  + "      }\n"
                                  + "    ]\n"
                                  + "  }", Predicate.class);
  }

  private Download testDownload() {
    Download download = new Download();

    PredicateDownloadRequest downloadRequest = new PredicateDownloadRequest();
    downloadRequest.setType(DownloadType.OCCURRENCE);
    downloadRequest.setFormat(DownloadFormat.DWCA);
    downloadRequest.setCreator("tester");
    downloadRequest.setSendNotification(true);
    downloadRequest.setPredicate(testPredicate());

    DOI doi = new DOI();
    doi.setPrefix(DOI.TEST_PREFIX);
    doi.setSuffix("dl.63cnym");

    download.setKey("1");
    download.setStatus(Download.Status.SUCCEEDED);
    download.setDoi(doi);
    download.setRequest(downloadRequest);
    download.setCreated(new Date());
    download.setModified(new Date());
    download.setLicense(License.CC_BY_4_0);
    download.setNumberDatasets(1L);
    download.setTotalRecords(1);
    download.setNumberDatasets(1L);

    return download;
  }

  @SneakyThrows
  private URI testDownloadLink(Download download) {
    return new URI("http://test.gbif.org/v1" + download.getKey());
  }

  private TitleLookupService mockTitleLookupService(List<ConstituentDataset> constituentDatasets) {
    TitleLookupService mockTitleLookupService = mock(TitleLookupService.class);
    when(mockTitleLookupService.getDatasetTitle(anyString())).thenAnswer(new Answers.GetDatasetTileAnswer(constituentDatasets));
    when(mockTitleLookupService.getSpeciesName(anyString())).thenReturn("Species name 1");
    return mockTitleLookupService;
  }

  @Test
  public void metadataBuildTest() {
    Download download = testDownload();
    List<ConstituentDataset> constituentDatasets = Datasets.testConstituentsDatasets(3);

    DownloadMetadataBuilder metadataBuilder = DownloadMetadataBuilder.builder()
                                                .archiveDir(tempDir.toFile())
                                                .download(download)
                                                .titleLookup(mockTitleLookupService(constituentDatasets))
                                                .downloadLinkProvider(this::testDownloadLink)
                                                .build();

    constituentDatasets.forEach(metadataBuilder);

    metadataBuilder.writeMetadata();

    //Assert metadata.xml exist
    File metadataFile = new File(tempDir.toFile(), DwcDownloadsConstants.METADATA_FILENAME);
    assertTrue(metadataFile.exists(), "Metadata file doesn't exist");

  }
}
