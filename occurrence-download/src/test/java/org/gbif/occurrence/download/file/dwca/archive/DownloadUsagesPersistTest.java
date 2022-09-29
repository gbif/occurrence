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

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;

import org.junit.jupiter.api.Test;

import static org.gbif.occurrence.download.file.dwca.archive.Datasets.testUsages;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for class DownloadUsagesPersist.
 */
public class DownloadUsagesPersistTest {

  /**
   * Tests the DownloadUsagesPersist.createUsages method.
   */
  @Test
  public void persistUsagesTest() {
    //Creates mock instances
    OccurrenceDownloadService occurrenceDownloadService = mock(OccurrenceDownloadService.class);
    DownloadUsagesPersist downloadUsagesPersist = DownloadUsagesPersist.create(occurrenceDownloadService);

    //Persists test usages
    downloadUsagesPersist.persistUsages("1", testUsages(3));

    //Verify the mock occurrence download service is called
    verify(occurrenceDownloadService, times(1)).createUsages(anyString(), any());
  }

  /**
   * Tests the DownloadUsagesPersist.update method.
   */
  @Test
  public void persistDownloadLicenseTest() {
    //Creates mock instances
    OccurrenceDownloadService occurrenceDownloadService = mock(OccurrenceDownloadService.class);
    DownloadUsagesPersist downloadUsagesPersist = DownloadUsagesPersist.create(occurrenceDownloadService);

    //Persists a download with a new license
    Download download = new Download();
    download.setLicense(License.UNSPECIFIED);
    downloadUsagesPersist.persistDownloadLicense(download, License.CC_BY_4_0);

    //Verify the license has changed
    assertEquals(download.getLicense(), License.CC_BY_4_0);

    //Verify the mock occurrence download service is called
    verify(occurrenceDownloadService, times(1)).update(any());
  }
}
