package org.gbif.occurrence.persistence.hbase;

import org.gbif.utils.number.BigDecimalUtils;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Simple test to check the size on {@link BigDecimal} in bytes using HBase {@link Bytes} class.
 */
public class BigDecimalTest {

  @Test
  public void testBigDecimalWithHBaseBytes(){
    double value = 2.5444444d;
    // rounded BigDecimal takes less byte of storage
    BigDecimal number = BigDecimalUtils.fromDouble(value, false);
    BigDecimal numberRounded = number.setScale(1, BigDecimal.ROUND_HALF_UP);
    assertTrue(Bytes.toBytes(numberRounded).length < Bytes.toBytes(number).length );

    // rounded BigDecimal takes less byte of storage than double
    assertTrue(Bytes.toBytes(numberRounded).length < Bytes.toBytes(value).length );
  }
}
