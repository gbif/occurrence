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

import java.io.Closeable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Builder;
import lombok.SneakyThrows;

import static org.gbif.occurrence.download.file.dwca.archive.Datasets.testDatasets;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Utility class to test constituent datasets based download files.
 */
public class ConstituentsFileTester<T extends Closeable & Consumer<Dataset>> {

  private final int numberOfTestDatasets;

  private final Path tempDir;

  private final String testFile;

  private final Supplier<T> writerSupplier;

  private final ConstituentFileAssertion.ConstituentFileAssertionBuilder assertionBuilder;

  /**
   * Counts \n in rights pattern.
   */
  public static int newLinesInPattern(String pattern) {
    int count = (int)pattern.chars().filter(ch -> ch == '\n').count();
    return count == 0? 1 : count + 1;
  }

  @Builder
  public ConstituentsFileTester(
    int numberOfTestDatasets,
    Path tempDir,
    String testFile,
    Function<Dataset, String> textProvider,
    Supplier<T> writerSupplier,
    int linesPerDataset,
    int linesInHeader
  ) {
    this.numberOfTestDatasets = numberOfTestDatasets;
    this.tempDir = tempDir;
    this.testFile = testFile;
    this.writerSupplier = writerSupplier;

    assertionBuilder =  ConstituentFileAssertion.builder()
      .textProvider(textProvider)
      .linesInHeader(linesInHeader)
      .linesPerDataset(linesPerDataset);
  }

  /**
   * Reads the rights file into a lists of Strings.
   */
  @SneakyThrows
  private List<String> readTestFile() {
    return Files.readAllLines(new File(tempDir.toFile(), testFile).toPath());
  }

  @SneakyThrows
  public void testFileContent() {
    List<Dataset> datasets = testDatasets(numberOfTestDatasets);

    //Creates a test file
    try (T writer = writerSupplier.get()) {
      datasets.forEach(writer);
    }

    List<String> fileLines = readTestFile();

    assertionBuilder
      .datasets(datasets)
      .fileLines(fileLines)
      .build()
      .testFileContent();
  }

  @Builder
  public static class ConstituentFileAssertion {

    private final List<Dataset> datasets;
    private final List<String> fileLines;
    private final int linesPerDataset;
    private final int linesInHeader;
    private final Function<Dataset, String> textProvider;

    /**
     * Gets the content from rightsStartIdx to startIdx + linesPerDataset from fileLines.
     */
    private String getContent(int startIdx, int linesPerDataset, List<String> fileLines) {
      return IntStream.range(startIdx, startIdx + linesPerDataset)
        .boxed()
        .map(fileLines::get)
        .collect(Collectors.joining("\n"));
    }

    /**
     * Is the file content expected to the dataset rights text for each dataset.
     */
    private void assertFileContent(List<Dataset> datasets, List<String> fileLines) {
      IntStream.range(0, datasets.size())
        .forEach(idx -> {
          int startIdx = linesInHeader + idx * linesPerDataset;
          String generatedRights = getContent(startIdx, linesPerDataset, fileLines);
          assertEquals(textProvider.apply(datasets.get(idx)), generatedRights);
        });
    }

    @SneakyThrows
    public void testFileContent() {

      assertFileContent(datasets, fileLines);

      //times linesInPattern() because of new lines in generated text
      assertEquals(datasets.size() * linesPerDataset, (fileLines.size() - linesInHeader));
    }
  }

}
