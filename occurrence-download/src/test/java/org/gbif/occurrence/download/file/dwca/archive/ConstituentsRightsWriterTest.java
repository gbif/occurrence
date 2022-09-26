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

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.SneakyThrows;

import static org.gbif.occurrence.download.file.dwca.archive.ConstituentsRightsWriter.RIGHTS_LINE_TEMPLATE;

public class ConstituentsRightsWriterTest {

  @TempDir
  static Path tempDir;

  @SneakyThrows
  @Test
  public void rightWriteTests() {
    ConstituentsFileTester.builder()
      .numberOfTestDatasets(3)
      .tempDir(tempDir)
      .testFile(DwcDownloadsConstants.RIGHTS_FILENAME)
      .writerSupplier(() -> new ConstituentsRightsWriter(tempDir.toFile()))
      .textProvider(ConstituentsRightsWriter::datasetRights)
      .linesInHeader(0)
      .linesPerDataset(ConstituentsFileTester.newLinesInPattern(RIGHTS_LINE_TEMPLATE))
      .build()
      .testFileContent();
  }
}
