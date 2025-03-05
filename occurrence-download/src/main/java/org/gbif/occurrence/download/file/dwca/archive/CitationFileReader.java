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

import org.gbif.api.model.registry.DatasetCitation;
import org.gbif.api.service.registry.DatasetService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Strings;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class CitationFileReader {

  private final FileSystem sourceFs;
  private final String citationFileName;
  private final DatasetService datasetService;
  private final Consumer<ConstituentDataset> onRead;
  private final Consumer<Map<UUID,Long>> onFinish;
  private final Map<UUID,Long> datasetUsages = new HashMap<>();

  /**
   * Creates Map with dataset UUIDs and its record counts.
   */
  public void read() throws IOException {
    Path citationSrc = new Path(citationFileName);
    // the hive query result is a directory with one or more files - read them all into an uuid set
    if (sourceFs.exists(citationSrc)) { // Empty results can lead to empty citation files
      FileStatus[] citFiles = sourceFs.listStatus(citationSrc);
      int invalidUuids = 0;
      for (FileStatus fs : citFiles) {
        if (!fs.isDirectory()) {
          log.info("Reading citation file {}", fs);
          try (BufferedReader citationReader = new BufferedReader(new InputStreamReader(sourceFs.open(fs.getPath()),
                                                                                        StandardCharsets.UTF_8))) {
            String line = citationReader.readLine();
            Map<UUID, Long> uuids = new HashMap<>();
            List<ConstituentDataset> constituents = new ArrayList<>();

            while (line != null) {
              if (!Strings.isNullOrEmpty(line)) {
                // we also catch errors for every dataset to don't break the loop
                try {
                  String[] constituentLine = line.split("\t");
                  uuids.put(
                    UUID.fromString(constituentLine[0]),
                    Long.parseLong(constituentLine[1])
                  );
                } catch (Exception e) {
                  // ignore invalid UUIDs
                  log.info("Found invalid UUID as datasetId {}", line, e);
                  invalidUuids++;
                }
              }
              line = citationReader.readLine();
            }

            // lookups for all datasets in one go
            List<DatasetCitation> datasetCitations = datasetService.listCitations(
              new ArrayList<>(uuids.keySet()));

            datasetCitations.forEach(datasetCitation -> {
              ConstituentDataset constituent = ConstituentDataset.builder()
                .key(datasetCitation.getKey())
                .records(uuids.get(datasetCitation.getKey()))
                .datasetCitation(datasetCitation)
                .build();
              constituents.add(constituent);
            });

            for (ConstituentDataset constituent : constituents) {
              datasetUsages.put(constituent.getKey(), constituent.getRecords());
              onRead.accept(constituent);
            }
          }
        }
      }
      if (invalidUuids > 0) {
        log.info("Found {} invalid dataset UUIDs", invalidUuids);
      }
    }
    onFinish.accept(datasetUsages);
  }
}
