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

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NestedStructConfigUtilTest {

  @Test
  public void parsesStringMap() {
    assertEquals(
        Map.of("7ddf754f-d193-4cc9-b351-99906754a03b", "gbif_classification"),
        NestedStructConfigUtil.parse(
            "{\"7ddf754f-d193-4cc9-b351-99906754a03b\": \"gbif_classification\"}", "config"));
    assertTrue(NestedStructConfigUtil.parse("{}", "config").isEmpty());
  }

  @Test
  public void rejectsInvalidJson() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> NestedStructConfigUtil.parse("{", "config"));
    assertEquals("Invalid config JSON format", e.getMessage());
  }

  @Test
  public void rejectsNonStringValues() {
    // fails when parsing, instead of with a ClassCastException when the value is used
    assertThrows(
        IllegalArgumentException.class,
        () -> NestedStructConfigUtil.parse("{\"key\": {\"nested\": \"value\"}}", "config"));
    assertThrows(
        IllegalArgumentException.class,
        () -> NestedStructConfigUtil.parse("{\"key\": [\"value\"]}", "config"));
  }
}
