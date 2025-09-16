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
package org.gbif.occurrence.download.hive;

import static org.junit.Assert.assertTrue;

import org.gbif.api.model.Constants;
import org.gbif.occurrence.download.sql.DownloadQueryParameters;
import org.junit.Test;

public class GenerateHQLTest {

  @Test
  public void simpleCsvTest() throws Exception {
    String simpleCsvDownloadQuery =
        GenerateHQL.simpleCsvQueryHQL(
            DownloadQueryParameters.builder()
                .checklistKey(Constants.NUB_DATASET_KEY.toString())
                .build());
    System.out.println(simpleCsvDownloadQuery);
  }

  @Test
  public void dwcaTest() throws Exception {
    String dwcaDownloadQuery =
        GenerateHQL.generateDwcaQueryHQL(
            DownloadQueryParameters.builder()
                .checklistKey(Constants.NUB_DATASET_KEY.toString())
                .build());
    System.out.println(dwcaDownloadQuery);
  }

  @Test
  public void simpleParquetTest() throws Exception {
    String simpleParquetDownloadQuery =
        GenerateHQL.simpleParquetQueryHQL(
            DownloadQueryParameters.builder()
                .checklistKey(Constants.NUB_DATASET_KEY.toString())
                .build());
    System.out.println(simpleParquetDownloadQuery);

    assertTrue(
        "Column names should be lower-case",
        simpleParquetDownloadQuery.contains("`datasetkey` STRING"));
    assertTrue(
        "Verbatim column names should be lower-case",
        simpleParquetDownloadQuery.contains("`verbatimscientificname` STRING"));
  }
}
