package org.gbif.occurrence.persistence;

import org.gbif.service.exception.PersistenceException;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an iterator over occurrence keys for a given HBase scan. To guarantee that all resources are released after
 * use, please make sure to iterate over the entire result (until hasNext() returns false).
 */
public class OccurrenceKeyIterator implements Iterator<Integer>, AutoCloseable {

  private final ResultScanner scanner;
  private final Iterator<Result> iterator;
  private boolean scannerClosed = false;

  /**
   * Create the iterator from a given tablePool, for the specified occurrence table, and the given scan. Only the row
   * keys for the returned scan are returned in calls to next().
   *
   * @param connection the HBase connection
   * @param occurrenceTableName the occurrence table to scan
   * @param scan the scan (query) to execute
   */
  public OccurrenceKeyIterator(Connection connection, String occurrenceTableName, Scan scan) {
    // TODO: heartbeat thread to shutdown/close resources if no activity after x seconds?

    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      scanner = table.getScanner(scan);
      iterator = scanner.iterator();
    } catch (IOException e) {
      throw new PersistenceException("Could not read from HBase", e);
    }
  }

  @Override
  public boolean hasNext() {
    if (scannerClosed) {
      return false;
    }

    if (iterator.hasNext()) {
      return true;
    } else {
      close();
      return false;
    }
  }

  @Override
  public Integer next() {
    if (hasNext()) {
     Result result = iterator.next();
     return Bytes.toInt(result.getRow());
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("This iterator does not support removal.");
  }

  @Override
  public void close() {
    scanner.close();
    scannerClosed = true;
  }
}
