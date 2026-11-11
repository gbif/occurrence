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
package org.gbif.occurrence.search;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchResponseTest {

  @Test
  void resultsKeepTheOrderOfTheKeysAndListTheKeysNotFound() {
    BatchResponse<Long, String> response =
        BatchResponse.of(Arrays.asList(3L, 1L, null, 2L, 3L, 4L), Map.of(1L, "one", 3L, "three"));

    assertEquals(List.of("three", "one"), response.getResults());
    assertEquals(List.of(2L, 4L), response.getNotFound());
  }

  @Test
  void noKeysReturnAnEmptyResponse() {
    BatchResponse<Long, String> response = BatchResponse.of(null, Map.of());

    assertEquals(List.of(), response.getResults());
    assertEquals(List.of(), response.getNotFound());
  }

  @Test
  void distinctKeysAreLimited() {
    assertEquals(List.of(1L, 2L), BatchResponse.distinctKeys(List.of(1L, 2L, 1L, 2L), 2));
    assertThrows(IllegalArgumentException.class, () -> BatchResponse.distinctKeys(List.of(1L, 2L, 3L), 2));
  }
}
