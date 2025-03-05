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

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gbif.api.model.registry.DatasetCitation;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class Answers {

  private static final String TEST_EML_PATH = "/eml/eml-test.xml";

  static Path getTestEmlPath() {
    return Paths.get(Answers.class.getResource(TEST_EML_PATH).getPath());
  }

  /**
   * Mocks datasetService.getMetadataDocument(UUID)
   */
  static class MetadataDocumentAnswer implements Answer<InputStream> {

    @Override
    public InputStream answer(InvocationOnMock invocation) {
      return getClass().getResourceAsStream(TEST_EML_PATH);
    }
  }

  /**
   * Mocks datasetService.get(UUID)
   */
  static class GetDatasetAnswer implements Answer<Dataset> {
    @Override
    public Dataset answer(InvocationOnMock invocation) {
      Dataset dataset = new Dataset();
      dataset.setKey(invocation.getArgument(0));
      dataset.setTitle("Dataset " + dataset.getKey());
      return dataset;
    }
  }


  /**
   * Mocks TitleLookupService.getDatasetTitle(UUID)
   */
  static class GetDatasetTileAnswer implements Answer<String> {

    private final Map<UUID, DatasetCitation> datasetMap;

    GetDatasetTileAnswer(List<ConstituentDataset> constituentDatasets) {
      datasetMap = constituentDatasets.stream()
        .collect(Collectors.toMap(ConstituentDataset::getKey, ConstituentDataset::getDatasetCitation));
    }

    @Override
    public String answer(InvocationOnMock invocation) {
      return datasetMap.get(invocation.getArgument(0))
        .getTitle();
    }
  }
}
