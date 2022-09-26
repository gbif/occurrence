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

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.SneakyThrows;

import static org.gbif.occurrence.download.file.dwca.archive.ConstituentsRightsWriter.RIGHTS_LINE_TEMPLATE;
import static org.mockito.Mockito.mock;

public class ConstituentsDatasetsProcessorTest {

  @TempDir
  static Path tempDir;

  private ConstituentsDatasetsProcessor testConstituentsDatasetsProcessor() {
    return ConstituentsDatasetsProcessor
            .builder()
            .archiveDir(tempDir.toFile())
            .datasetService(mock(DatasetService.class, new Answers.MetadataDocumentAnswer()))
            .build();
  }

  @SneakyThrows
  private List<String> readTestFile(File file) {
    return Files.readAllLines(file.toPath());
  }

  /**
   * Validates the produced rights file.
   */
  private void assertRightsFile(List<Dataset> datasets) {
    ConstituentsFileTester.ConstituentFileAssertion.builder()
      .textProvider(ConstituentsRightsWriter::datasetRights)
      .linesInHeader(0)
      .linesPerDataset(ConstituentsFileTester.newLinesInPattern(RIGHTS_LINE_TEMPLATE))
      .datasets(datasets)
      .fileLines(readTestFile(new File(tempDir.toFile(), DwcDownloadsConstants.RIGHTS_FILENAME)))
      .build()
      .testFileContent();
  }

  /**
   * Validates the produced citation file.
   */
  private void assertCitationsFile(List<Dataset> datasets) {
    ConstituentsFileTester.ConstituentFileAssertion.builder()
      .textProvider(ConstituentsCitationWriter::citation)
      .linesInHeader(1)
      .linesPerDataset(1)
      .datasets(datasets)
      .fileLines(readTestFile(new File(tempDir.toFile(), DwcDownloadsConstants.CITATIONS_FILENAME)))
      .build()
      .testFileContent();
  }

  @SneakyThrows
  @Test
  public void testConstituentsProcessor() {
    List<ConstituentDataset> constituentDatasets = Datasets.testConstituentsDatasets(3);
    List<Dataset> datasets = constituentDatasets.stream().map(ConstituentDataset::getDataset).collect(Collectors.toList());

    try (ConstituentsDatasetsProcessor constituentsDatasetsProcessor = testConstituentsDatasetsProcessor()) {
      constituentDatasets.forEach(constituentsDatasetsProcessor);
    }

    assertRightsFile(datasets);

    assertCitationsFile(datasets);

  }
}
