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
package org.gbif.metrics.ws.resources;

import java.util.Calendar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link OccurrenceCubeResource#parseYearRange(String)}. */
class OccurrenceCubeResourceTest {

  private static final int CURRENT_YEAR = Calendar.getInstance().get(Calendar.YEAR);

  @Test
  void testValidYearRange() {
    assertDoesNotThrow(() -> OccurrenceCubeResource.parseYearRange("1742"));
    assertDoesNotThrow(() -> OccurrenceCubeResource.parseYearRange("1742,1802"));
    assertDoesNotThrow(() -> OccurrenceCubeResource.parseYearRange("1500,1900"));
    assertDoesNotThrow(() -> OccurrenceCubeResource.parseYearRange("1000"));
    assertDoesNotThrow(() -> OccurrenceCubeResource.parseYearRange("1000," + (CURRENT_YEAR + 1)));
    assertDoesNotThrow(() -> OccurrenceCubeResource.parseYearRange(String.valueOf(CURRENT_YEAR)));
  }

  @Test
  void testIllegalYearRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OccurrenceCubeResource.parseYearRange("999"));
    assertThrows(
        IllegalArgumentException.class,
        () -> OccurrenceCubeResource.parseYearRange("12,13"));
    assertThrows(
        IllegalArgumentException.class,
        () -> OccurrenceCubeResource.parseYearRange("-321,1981"));
    assertThrows(
        IllegalArgumentException.class,
        () -> OccurrenceCubeResource.parseYearRange("1200,2100"));
    assertThrows(
        IllegalArgumentException.class,
        () -> OccurrenceCubeResource.parseYearRange("2040"));
    assertThrows(
        IllegalArgumentException.class,
        () -> OccurrenceCubeResource.parseYearRange("1,2,3"));
    assertThrows(
        NumberFormatException.class,
        () -> OccurrenceCubeResource.parseYearRange(""));
  }

  @Test
  void testNullYearRange() {
    assertThrows(
        NullPointerException.class,
        () -> OccurrenceCubeResource.parseYearRange(null));
  }
}
