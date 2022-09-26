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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConstituentsEmlWriter implements Closeable, Consumer<Dataset> {

  public static final String OUTPUT_DIRECTORY = "dataset";

  private final DatasetService datasetService;
  private final File emlDir;

  public ConstituentsEmlWriter(DatasetService datasetService, File archiveDir) {
    this.datasetService = datasetService;
    emlDir = new File(archiveDir, OUTPUT_DIRECTORY);
    emlDir.mkdir();
  }

  private void createEmlFile(UUID constituentId) {
    try (InputStream in = datasetService.getMetadataDocument(constituentId)) {
      // store dataset EML as constituent metadata
      if (in != null) {
        Files.copy(in, Paths.get(emlDir.getPath(), constituentId + ".xml"));
      } else {
        log.error("EML Not Found for datasetId {}", constituentId);
      }
    } catch (IOException ex) {
      log.error("Error creating eml file", ex);
    }
  }

  @Override
  public void accept(Dataset dataset) {
    createEmlFile(dataset.getKey());
  }

  @Override
  public void close() throws IOException {
    if (isEmpty(emlDir.toPath())) {
      log.info("Deleting datasets EML path {}", emlDir);
      emlDir.delete();
    }
  }

  /**
   * Is directory empty.
   */
  private static boolean isEmpty(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      try (Stream<Path> entries = Files.list(path)) {
        return !entries.findFirst().isPresent();
      }
    }
    return false;
  }

}
