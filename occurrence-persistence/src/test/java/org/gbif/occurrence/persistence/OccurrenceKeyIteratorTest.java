package org.gbif.occurrence.persistence;

import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

//@Ignore("As per http://dev.gbif.org/issues/browse/OCC-109")
public class OccurrenceKeyIteratorTest {

  private static final String TABLE_NAME = "occurrence_test";
  private static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final UUID DATASET_1 = UUID.randomUUID();
  private static final int ID_1_COUNT = 200;
  private static final UUID DATASET_2 = UUID.randomUUID();
  private static final int ID_2_COUNT = 750;
  private static final UUID DATASET_3 = UUID.randomUUID();
  private static final int ID_3_COUNT = 1200;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private HTablePool tablePool = null;

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);

    tablePool = new HTablePool(TEST_UTIL.getConfiguration(), 1);

    HTableInterface table = tablePool.getTable(TABLE_NAME);
    int keyCount = 0;
    for (int i = 0; i < ID_1_COUNT; i++) {
      keyCount++;
      addId(keyCount, DATASET_1.toString(), table);
    }
    for (int i = 0; i < ID_2_COUNT; i++) {
      keyCount++;
      addId(keyCount, DATASET_2.toString(), table);
    }
    for (int i = 0; i < ID_3_COUNT; i++) {
      keyCount++;
      addId(keyCount, DATASET_3.toString(), table);
    }
    table.flushCommits();
    table.close();
  }

  private void addId(int key, String value, HTableInterface table) throws IOException {
    Put put = new Put(Bytes.toBytes(key));
    put.add(CF, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATASET_KEY).getColumnName()),
      Bytes.toBytes(value));
    table.put(put);
  }

  @Test
  public void testOneRecord() throws IOException {
    TEST_UTIL.truncateTable(TABLE);
    HTableInterface table = tablePool.getTable(TABLE);
    addId(1, DATASET_1.toString(), table);
    table.flushCommits();
    table.close();

    byte[] col = Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATASET_KEY).getColumnName());
    Scan scan = new Scan();
    scan.addColumn(CF, col);
    scan.setBatch(500);
    scan.setCaching(500);
    SingleColumnValueFilter filter =
      new SingleColumnValueFilter(CF, col, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(DATASET_1.toString()));
    scan.setFilter(filter);

    Iterator<Integer> iterator = new OccurrenceKeyIterator(tablePool, TABLE_NAME, scan);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(1, count);
  }


  @Test
  public void testOnePage() {
    byte[] col = Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATASET_KEY).getColumnName());
    Scan scan = new Scan();
    scan.addColumn(CF, col);
    scan.setBatch(500);
    scan.setCaching(500);
    SingleColumnValueFilter filter =
      new SingleColumnValueFilter(CF, col, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(DATASET_1.toString()));
    scan.setFilter(filter);

    Iterator<Integer> iterator = new OccurrenceKeyIterator(tablePool, TABLE_NAME, scan);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(ID_1_COUNT, count);
  }

  @Test
  public void testMultiPage() {
    byte[] col = Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.DATASET_KEY).getColumnName());
    Scan scan = new Scan();
    scan.addColumn(CF, col);
    scan.setBatch(500);
    scan.setCaching(500);
    SingleColumnValueFilter filter =
      new SingleColumnValueFilter(CF, col, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(DATASET_3.toString()));
    scan.setFilter(filter);

    Iterator<Integer> iterator = new OccurrenceKeyIterator(tablePool, TABLE_NAME, scan);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(ID_3_COUNT, count);
  }
}
