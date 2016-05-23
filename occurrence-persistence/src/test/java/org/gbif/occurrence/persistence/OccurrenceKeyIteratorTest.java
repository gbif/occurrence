package org.gbif.occurrence.persistence;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OccurrenceKeyIteratorTest {

  private static final String TEST_TABLE_NAME = "occurrence_test";
  private static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE_NAME);
  private static final byte[] TABLE = Bytes.toBytes(TEST_TABLE_NAME);
  private static final String CF_NAME = "o";
  private static final byte[] CF = Bytes.toBytes(CF_NAME);
  private static final UUID DATASET_1 = UUID.randomUUID();
  private static final int ID_1_COUNT = 200;
  private static final UUID DATASET_2 = UUID.randomUUID();
  private static final int ID_2_COUNT = 750;
  private static final UUID DATASET_3 = UUID.randomUUID();
  private static final int ID_3_COUNT = 1200;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static Connection CONNECTION = null;

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if (CONNECTION != null) {
      CONNECTION.close();
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, CF);
    CONNECTION = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }


  @Before
  public void setUp() throws Exception {
    TEST_UTIL.truncateTable(TABLE);


    try (Table table = CONNECTION.getTable(TABLE_NAME)) {
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
    }
  }

  private void addId(int key, String value, Table table) throws IOException {
    Put put = new Put(Bytes.toBytes(key));
    put.addColumn(CF, Bytes.toBytes(Columns.column(GbifTerm.datasetKey)),
      Bytes.toBytes(value));
    table.put(put);
  }

  @Test
  public void testOneRecord() throws IOException {
    TEST_UTIL.truncateTable(TABLE);
    try (Table table = CONNECTION.getTable(TABLE_NAME)) {
      addId(1, DATASET_1.toString(), table);
    }

    byte[] col = Bytes.toBytes(Columns.column(GbifTerm.datasetKey));
    Scan scan = new Scan();
    scan.addColumn(CF, col);
    scan.setCaching(500);
    SingleColumnValueFilter filter =
      new SingleColumnValueFilter(CF, col, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(DATASET_1.toString()));
    scan.setFilter(filter);

    Iterator<Integer> iterator = new OccurrenceKeyIterator(CONNECTION, TABLE_NAME.getNameAsString(), scan);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(1, count);
  }


  @Test
  public void testOnePage() {
    byte[] col = Bytes.toBytes(Columns.column(GbifTerm.datasetKey));
    Scan scan = new Scan();
    scan.addColumn(CF, col);
    scan.setCaching(500);
    SingleColumnValueFilter filter = new SingleColumnValueFilter(CF, col, CompareFilter.CompareOp.EQUAL,
                                                                 Bytes.toBytes(DATASET_1.toString()));
    scan.setFilter(filter);

    Iterator<Integer> iterator = new OccurrenceKeyIterator(CONNECTION, TABLE_NAME.getNameAsString(), scan);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(ID_1_COUNT, count);
  }

  @Test
  public void testMultiPage() {
    byte[] col = Bytes.toBytes(Columns.column(GbifTerm.datasetKey));
    Scan scan = new Scan();
    scan.addColumn(CF, col);
    scan.setCaching(500);
    SingleColumnValueFilter filter = new SingleColumnValueFilter(CF, col, CompareFilter.CompareOp.EQUAL,
                                                                 Bytes.toBytes(DATASET_3.toString()));
    scan.setFilter(filter);

    Iterator<Integer> iterator = new OccurrenceKeyIterator(CONNECTION, TABLE_NAME.getNameAsString(), scan);
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    assertEquals(ID_3_COUNT, count);
  }
}
