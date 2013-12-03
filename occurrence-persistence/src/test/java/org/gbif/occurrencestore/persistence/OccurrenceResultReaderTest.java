package org.gbif.occurrencestore.persistence;

import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.hbase.HBaseFieldUtil;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class OccurrenceResultReaderTest {

  private static final String CF1_NAME = "o";
  private static final byte[] CF1 = Bytes.toBytes(CF1_NAME);
  private static final int KEY_NAME = 12345;
  private static final byte[] KEY = Bytes.toBytes(KEY_NAME);

  private static final int INT_VAL_1 = 1111;
  private static final double DOUBLE_VAL_1 = 2.2222222222222222222d;
  private static final long LONG_VAL_1 = 33333333333333l;
  private static final String STRING_VAL_1 = "not numbers";

  private Result result = null;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    List<KeyValue> kvs = new ArrayList<KeyValue>();

    // mimic four fields of an occurrence row
    KeyValue kv = new KeyValue(KEY, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_PROVIDER_ID).getColumnFamilyName()),
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATA_PROVIDER_ID).getColumnName()), Bytes.toBytes(INT_VAL_1));
    kvs.add(kv);
    kv = new KeyValue(KEY, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_LATITUDE).getColumnFamilyName()),
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.I_LATITUDE).getColumnName()), Bytes.toBytes(DOUBLE_VAL_1));
    kvs.add(kv);
    kv = new KeyValue(KEY, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MODIFIED).getColumnFamilyName()),
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.MODIFIED).getColumnName()), Bytes.toBytes(LONG_VAL_1));
    kvs.add(kv);
    kv = new KeyValue(KEY, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.SCIENTIFIC_NAME).getColumnFamilyName()),
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.SCIENTIFIC_NAME).getColumnName()), Bytes.toBytes(STRING_VAL_1));
    kvs.add(kv);

    result = new Result(kvs);
  }

  @Test
  public void testKey() {
    Integer test = OccurrenceResultReader.getKey(result);
    assertEquals(KEY_NAME, test.intValue());
  }

  @Test
  public void testString() {
    String test = OccurrenceResultReader.getString(result, FieldName.SCIENTIFIC_NAME, null);
    assertEquals(STRING_VAL_1, test);
  }

  @Test
  public void testStringFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [MODIFIED] is not of type String");
    OccurrenceResultReader.getString(result, FieldName.MODIFIED, null);
  }

  @Test
  public void testInteger() {
    Integer test = OccurrenceResultReader.getInteger(result, FieldName.DATA_PROVIDER_ID, null);
    assertEquals(INT_VAL_1, test.intValue());
  }

  @Test
  public void testIntegerFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [MODIFIED] is not of type Integer");
    OccurrenceResultReader.getInteger(result, FieldName.MODIFIED, null);
  }

  @Test
  public void testLong() {
    Long test = OccurrenceResultReader.getLong(result, FieldName.MODIFIED, null);
    assertEquals(LONG_VAL_1, test.longValue());
  }

  @Test
  public void testLongFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [SCIENTIFIC_NAME] is not of type Long");
    OccurrenceResultReader.getLong(result, FieldName.SCIENTIFIC_NAME, null);
  }

  @Test
  public void testDouble() {
    Double test = OccurrenceResultReader.getDouble(result, FieldName.I_LATITUDE, null);
    assertEquals(DOUBLE_VAL_1, test, 0.00001);
  }

  @Test
  public void testDoubleFail() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("FieldName [SCIENTIFIC_NAME] is not of type Double");
    OccurrenceResultReader.getDouble(result, FieldName.SCIENTIFIC_NAME, null);
  }

  @Test
  public void testObject() {
    Object test = OccurrenceResultReader.get(result, FieldName.I_LATITUDE);
    assertEquals(DOUBLE_VAL_1, (Double) test, 0.00001);
  }
}
