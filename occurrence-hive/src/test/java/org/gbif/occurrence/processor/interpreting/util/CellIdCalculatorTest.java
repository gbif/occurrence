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
package org.gbif.occurrence.processor.interpreting.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CellIdCalculatorTest {

  @Test
  public void testCellIdSuccess() {
    Integer cellId = CellIdCalculator.calculateCellId(45.123d, 45.456d);
    assertEquals(48825, cellId.intValue());
  }

  @Test
  public void testCellIdNull() {
    Integer cellId = CellIdCalculator.calculateCellId(200d, 200d);
    assertNull(cellId);
  }

  @Test
  public void testCentiCellIdSuccess() {
    Integer cellId = CellIdCalculator.calculateCentiCellId(45.123d, 45.456d);
    assertEquals(14, cellId.intValue());
  }

  @Test
  public void testCentiCellIdNull() {
    Integer cellId = CellIdCalculator.calculateCentiCellId(200d, 200d);
    assertNull(cellId);
  }

  @Test
  public void testMod360Success() {
    Integer cellId = CellIdCalculator.calculateMod360CellId(45.456);
    assertEquals(225, cellId.intValue());
  }

  @Test
  public void testMod360Null() {
    Integer cellId = CellIdCalculator.calculateMod360CellId(null);
    assertNull(cellId);
  }
}
