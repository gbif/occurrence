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
package org.gbif.occurrence.common.identifier;


import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OccurrenceKeyHelperTest {

  private static final UUID DATASET_KEY = UUID.randomUUID();
  private static final String IC = "BGBM";
  private static final String CC = "Vascular Plants";
  private static final String CN = "00234-asdfa-234as-asdf-cvb";
  private static final String UQ = "Abies alba";
  private static final String DWC = "98098098-234asd-asdfa-234df";

  private static final HolyTriplet TRIPLET = new HolyTriplet(DATASET_KEY, IC, CC, CN, UQ);
  private static final PublisherProvidedUniqueIdentifier PUB_PROVIDED = new PublisherProvidedUniqueIdentifier(DATASET_KEY, DWC);

  @Test
  public void testTripletKey() {
    String testKey = OccurrenceKeyHelper.buildKey(TRIPLET);
    assertEquals(DATASET_KEY.toString() + "|" + IC + "|" + CC + "|" + CN + "|" + UQ, testKey);
  }

  @Test
  public void testSingleDwcKey() {
    String testKey = OccurrenceKeyHelper.buildKey(PUB_PROVIDED);
    assertEquals(DATASET_KEY.toString() + "|" + DWC, testKey);
  }
}
