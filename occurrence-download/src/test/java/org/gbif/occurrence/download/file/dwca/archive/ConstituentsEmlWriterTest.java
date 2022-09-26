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
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.SneakyThrows;

import static org.gbif.occurrence.download.file.dwca.archive.ConstituentsEmlWriter.OUTPUT_DIRECTORY;
import static org.gbif.occurrence.download.file.dwca.archive.Datasets.testDatasets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Eml dataset creation tests.
 */
public class ConstituentsEmlWriterTest {

  @TempDir
  Path tempDir;

  /**
   * Creates the mock DatasetService using a single answer.
   */
  private DatasetService mockDatasetService() {
    return mock(DatasetService.class, new Answers.MetadataDocumentAnswer());
  }

  /**
   * Size of file in bytes.
   */
  @SneakyThrows
  private long fileSize(Path file) {
    return Files.size(file);
  }

  /**
   * Gets the EML documents for each dataset and writes them to the dataset directory.
   */
  @SneakyThrows
  private void createEmlFiles(List<Dataset> testDatasets) {
    try(ConstituentsEmlWriter emlWriter = new ConstituentsEmlWriter(mockDatasetService(), tempDir.toFile())) {
      testDatasets.forEach(emlWriter);
    }
  }

  /**
   * Test that EML files are created correctly.
   */
  @SneakyThrows
  @Test
  public void emlWriteTest() {
    //2 test datasets
    List<Dataset> testDatasets = testDatasets(2);

    //create EML files
    createEmlFiles(testDatasets);

    //datasets directory exists
    File datasetsDir = new File(tempDir.toFile(), OUTPUT_DIRECTORY);
    assertTrue(datasetsDir.exists(), "Datasets directory doesn't exist");

    //Each eml file was created and has the same size as the test file
    long testFileSize = fileSize(Answers.getTestEmlPath());
    testDatasets.forEach(dataset -> {
      File emlDatasetFile = new File(datasetsDir, dataset.getKey() + ".xml");
      assertTrue(emlDatasetFile.exists());
      assertEquals(testFileSize, fileSize(emlDatasetFile.toPath()));
    });
  }

  /**
   * Tests the output dataset directory is not created when datasets are empty or not found.
   */
  @Test
  public void emlEmptyWriteTest() {
    createEmlFiles(Collections.emptyList());
    //datasets directory DOES NOT exist
    File datasetsDir = new File(tempDir.toFile(), OUTPUT_DIRECTORY);
    assertFalse(datasetsDir.exists(), "Dataset directory exists");
  }

}
