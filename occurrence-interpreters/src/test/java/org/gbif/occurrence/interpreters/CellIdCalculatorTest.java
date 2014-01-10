package org.gbif.occurrence.interpreters;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
