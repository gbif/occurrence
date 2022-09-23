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
package org.gbif.occurrence.download.file.dwca.oozie;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;

import java.io.IOException;
import java.util.Collections;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Oozie Java action that creates a DwCA from Hive tables.
 * This action is used for big DwCA downloads.
 */
@UtilityClass
@Slf4j
public class ArchiveDownloadAction {

  /**
   * Entry point for assembling the DWC archive.
   * The thrown exception is the only way of telling Oozie that this job has failed.
   *
   * The expected parameters are:
   * 0. downloadKey: occurrence download key.
   * 1. username: download user
   * 2. predicate: download query filter
   * 3. isSmallDownload
   * 4. download table/file name
   *
   * @throws java.io.IOException if any read/write operation failed
   */
  public static void main(String[] args) throws IOException {
    String downloadKey = args[0];
    String username = args[1];          // download user
    String query = args[2];         // download query filter
    boolean isSmallDownload = Boolean.parseBoolean(args[3]);    // isSmallDownload
    String downloadTableName = args[4];    // download table/file name
    DwcTerm dwcTerm = DwcTerm.valueOf(args[5]);

    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();

    DownloadJobConfiguration configuration = DownloadJobConfiguration.builder()
                                                .downloadKey(downloadKey)
                                                .downloadTableName(downloadTableName)
                                                .filter(query)
                                                .isSmallDownload(isSmallDownload)
                                                .user(username)
                                                .sourceDir(workflowConfiguration.getHiveDBPath())
                                                .downloadFormat(workflowConfiguration.getDownloadFormat())
                                                .coreTerm(dwcTerm)
                                                .extensions(Collections.emptySet())
                                                .build();

    log.info("DwcaArchiveBuilder instance created with parameters:{}", (Object)args);
    DwcaArchiveBuilder.of(configuration, workflowConfiguration).buildArchive();
  }
}
