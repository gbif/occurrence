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

import org.gbif.api.service.registry.DatasetService;
import org.gbif.occurrence.download.file.dwca.akka.CitationsFileWriter;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.SneakyThrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for citation file reads.
 */
public class CitationFileReaderTest {

  @TempDir
  static Path tempDir;

  /**
   * Mock DatasetService.get(UUID) method.
   */
  private DatasetService mockDatasetService() {
    DatasetService datasetService = mock(DatasetService.class);
    when(datasetService.get(any())).thenAnswer(new Answers.GetDatasetAnswer());
    return  datasetService;
  }

  /**
   * Local file system.
   */
  @SneakyThrows
  private FileSystem getLocalFileSystem() {
    return FileSystem.getLocal(new Configuration());
  }

  /**
   * Creates a map with random UUIDs and counts from the index.
   */
  private Map<UUID,Long> testMap(int length) {
    return LongStream.range(0, length)
            .boxed()
            .collect(Collectors.toMap(count -> UUID.randomUUID(), Function.identity()));
  }

  /**
   * Creates a test citation file in the temp test directory.
   */
  private File createCitationTestFile(int numberOfCitations) {
    File citationFile = new File(tempDir.toFile(),"citation.txt");
    Map<UUID,Long> citations = testMap(numberOfCitations);
    CitationsFileWriter.createCitationsFile(citations, citationFile.getAbsolutePath());
    return citationFile;
  }

  @SneakyThrows
  private void testTemplate(int numberOfCitations, File citationTestFile) {
    //Accumulate results
    List<ConstituentDataset> constituentDatasets = new ArrayList<>();

    //Create a file reader from the test files
    CitationFileReader.builder()
      .datasetService(mockDatasetService())
      .onRead(constituentDatasets::add)
      .sourceFs(getLocalFileSystem())
      .citationFileName(citationTestFile.getAbsolutePath())
      //Resulted citation have the expected quantity
      .onFinish(citations ->
                  assertEquals(numberOfCitations, citations.size(), "Expected citations differ"))
      .build()
      .read();

    //Resulted constituents have the expected quantity
    assertEquals(numberOfCitations, constituentDatasets.size(), "Expected citations differ");
  }

  /**
   * Tests that writes and reads and consistent for a citation file.
   */
  @Test
  @SneakyThrows
  public void readTest() {
    int numberOfCitations = 3;
    testTemplate(numberOfCitations, createCitationTestFile(numberOfCitations));
  }

  @Test
  @SneakyThrows
  public void emptyReadTest() {
    testTemplate(0, new File("thisFileDoesNotExist"));
  }
}
