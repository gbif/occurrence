package org.gbif.occurrence.persistence;

import org.gbif.occurrence.persistence.hbase.ExtResultReader;
import org.gbif.occurrence.persistence.hbase.FieldName;
import org.gbif.occurrence.persistence.hbase.FieldNameUtil;
import org.gbif.occurrence.persistence.hbase.HBaseColumn;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class ExtResultReaderTest {

  private static final int KEY_NAME = 12345;
  private static final byte[] KEY = Bytes.toBytes(KEY_NAME);

  private static final int INT_VAL_1 = 1111;
  private static final double DOUBLE_VAL_1 = 2.2222222222222222222d;
  private static final long LONG_VAL_1 = 33333333333333L;
  private static final String STRING_VAL_1 = "just a string";

  private Result result = null;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() throws InterruptedException {
    List<KeyValue> kvs = Lists.newArrayList();

    // mimic four fields of an occurrence row
    kvs.add(buildKv(FieldName.I_ELEVATION_ACC, Bytes.toBytes(INT_VAL_1)));
    kvs.add(buildKv(FieldName.I_DECIMAL_LONGITUDE, Bytes.toBytes(DOUBLE_VAL_1)));
    kvs.add(buildKv(FieldName.I_STATE_PROVINCE, Bytes.toBytes(STRING_VAL_1)));
    kvs.add(buildKv(FieldName.LAST_PARSED, Bytes.toBytes(LONG_VAL_1)));

    result = new Result(kvs);
  }

  private static KeyValue buildKv(FieldName fieldName, byte[] value) {
    HBaseColumn hbaseCol = FieldNameUtil.getColumn(fieldName);
    KeyValue kv =
      new KeyValue(KEY, Bytes.toBytes(hbaseCol.getFamilyName()), Bytes.toBytes(hbaseCol.getColumnName()), value);
    return kv;
  }

  @Test
  public void testKey() {
    Integer test = ExtResultReader.getKey(result);
    assertEquals(KEY_NAME, test.intValue());
  }

  @Test
  public void testString() {
    String test = ExtResultReader.getString(result, FieldName.I_STATE_PROVINCE, null);
    assertEquals(STRING_VAL_1, test);
  }

  @Test
  public void testStringFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [LAST_PARSED] is not of type String");
    ExtResultReader.getString(result, FieldName.LAST_PARSED, null);
  }

  @Test
  public void testInteger() {
    Integer test = ExtResultReader.getInteger(result, FieldName.I_ELEVATION_ACC, null);
    assertEquals(INT_VAL_1, test.intValue());
  }

  @Test
  public void testIntegerFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [LAST_PARSED] is not of type Integer");
    ExtResultReader.getInteger(result, FieldName.LAST_PARSED, null);
  }

  @Test
  public void testLong() {
    Long test = ExtResultReader.getLong(result, FieldName.LAST_PARSED, null);
    assertEquals(LONG_VAL_1, test.longValue());
  }

  @Test
  public void testLongFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [I_WATERBODY] is not of type Long");
    ExtResultReader.getLong(result, FieldName.I_WATERBODY, null);
  }

  @Test
  public void testDouble() {
    Double test = ExtResultReader.getDouble(result, FieldName.I_DECIMAL_LONGITUDE, null);
    assertEquals(DOUBLE_VAL_1, test, 0.00001);
  }

  @Test
  public void testDoubleFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [I_WATERBODY] is not of type Double");
    ExtResultReader.getDouble(result, FieldName.I_WATERBODY, null);
  }

}
