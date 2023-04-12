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
package org.gbif.occurrence.download.util;

import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.utils.file.FileUtils;

import java.io.File;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Common methods  to handle DwC-A downloads.
 */
@Slf4j
@UtilityClass
public class ArchiveFileUtils {


  public static void initializeArchiveDir(File archiveDir, DownloadJobConfiguration configuration) {
    if (!configuration.isSmallDownload()) {
      // oozie might try several times to run this job, so make sure our filesystem is clean
      cleanupFS(archiveDir);

      // create the temp archive dir
      archiveDir.mkdirs();
    }
  }

  /**
   * Removes all temporary file system artifacts but the final zip archive.
   */
  public static void cleanupFS(File archiveDir) {
    log.info("Cleaning up archive directory {}", archiveDir.getPath());
    if (archiveDir.exists()) {
      FileUtils.deleteDirectoryRecursively(archiveDir);
    }
  }
}
