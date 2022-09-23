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
import org.gbif.occurrence.download.file.DownloadJobConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class CitationFileReader {

  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();

  private final FileSystem sourceFs;
  private final DownloadJobConfiguration configuration;
  private final DatasetService datasetService;
  private final Consumer<ConstituentDataset> onRead;
  private final Consumer<Map<UUID,Long>> onFinish;
  private final Map<UUID,Long> datasetUsages = Maps.newHashMap();


  private ConstituentDataset parseConstituent(String line) {
    Iterator<String> iter = TAB_SPLITTER.split(line).iterator();
    // play safe and make sure we got an uuid - even though our api doesn't require it
    UUID key = UUID.fromString(iter.next());
    return ConstituentDataset.builder()
            .key(key)
            .records(Long.parseLong(iter.next()))
            .dataset(datasetService.get(key))
            .build();
  }


  /**
   * Creates Map with dataset UUIDs and its record counts.
   */
  public void read() throws IOException {
    Path citationSrc = new Path(configuration.getCitationDataFileName());
    // the hive query result is a directory with one or more files - read them all into an uuid set
    FileStatus[] citFiles = sourceFs.listStatus(citationSrc);
    int invalidUuids = 0;
    for (FileStatus fs : citFiles) {
      if (!fs.isDirectory()) {
        try (BufferedReader citationReader =
               new BufferedReader(new InputStreamReader(sourceFs.open(fs.getPath()), Charsets.UTF_8))) {

          String line = citationReader.readLine();
          while (line != null) {
            if (!line.isEmpty()) {
              // we also catch errors for every dataset to don't break the loop
              try {
                ConstituentDataset constituent = parseConstituent(line);
                datasetUsages.put(constituent.getKey(), constituent.getRecords());
                onRead.accept(constituent);
              } catch (Exception e) {
                // ignore invalid UUIDs
                log.info("Found invalid UUID as datasetId {}", line);
                invalidUuids++;
              }
            }
            line = citationReader.readLine();
          }
        }
      }
    }
    if (invalidUuids > 0) {
      log.info("Found {} invalid dataset UUIDs", invalidUuids);
    }
    onFinish.accept(datasetUsages);
  }
}
