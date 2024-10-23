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
package org.gbif.occurrence.spark.udf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MillisecondsToISO8601UDFTest {

  @Test
  public void testUtcDateFormatter() throws Exception {
    MillisecondsToISO8601Udf function = new MillisecondsToISO8601Udf();

    assertNull(function.call(null));
    assertEquals("2020-04-02T16:48:54Z", function.call(1_585_846_134_000L).toString());
    assertEquals("2020-04-02T16:48:54.001Z", function.call(1_585_846_134_001L).toString());
  }
}
