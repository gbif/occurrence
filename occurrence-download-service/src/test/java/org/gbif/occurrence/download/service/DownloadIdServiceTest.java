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
package org.gbif.occurrence.download.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DownloadIdServiceTest {

  @Test
  public void testId() {
    DownloadIdService downloadIdService = new DownloadIdService();

    //The first id is padded with 7 0s
    String id = downloadIdService.generateId();
    assertTrue(id.startsWith("0000000"));

    //The second id is 1 padded with 6 0s and so on
    String id2 = downloadIdService.generateId();
    assertTrue(id2.startsWith("0000001"));

    //Both ids use the same post-fix which is the start time
    assertEquals(id.substring(8), id2.substring(8));
  }
}
