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
import org.gbif.utils.file.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.function.Consumer;

import com.google.common.base.Strings;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.CITATIONS_FILENAME;

@Data
@Slf4j
public class ConstituentsCitationWriter implements Closeable, Consumer<Dataset> {
  private static final String CITATION_HEADER =
    "When using this dataset please use the following citation and pay attention to the rights documented in rights.txt:\n";

  private final Writer writer;

  @SneakyThrows
  public ConstituentsCitationWriter(File archiveDir) {
    writer = FileUtils.startNewUtf8File(new File(archiveDir, CITATIONS_FILENAME));
    // write fixed citations header
    writer.write(CITATION_HEADER);
  }

  @SneakyThrows
  private void writeCitation(Dataset dataset) {
    // citation
    if (dataset.getCitation() != null && !Strings.isNullOrEmpty(dataset.getCitation().getText())) {
      writer.write(dataset.getCitation().getText());
      writer.write('\n');
    } else {
      log.error("Constituent dataset misses mandatory citation for id: {}", dataset.getKey());
    }
  }

  @Override
  public void accept(Dataset dataset) {
    writeCitation(dataset);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
