package org.gbif.occurrence.persistence;

import org.gbif.service.exception.PersistenceException;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an iterator over occurrence keys for a given HBase scan. To guarantee that all resources are released after
 * use, please make sure to iterate over the entire result (until hasNext() returns false).
 */
public class OccurrenceKeyIterator implements Iterator<Integer>, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceKeyIterator.class);
  private final ResultScanner scanner;
  private final Iterator<Result> iterator;
  private final HTableInterface table;
  private boolean scannerClosed = false;

  /**
   * Create the iterator from a given tablePool, for the specified occurrence table, and the given scan. Only the row
   * keys for the returned scan are returned in calls to next().
   *
   * @param tablePool the HBase tablePool that holds the connection to HBase
   * @param occurrenceTableName the occurrence table to scan
   * @param scan the scan (query) to execute
   */
  public OccurrenceKeyIterator(HTablePool tablePool, String occurrenceTableName, Scan scan) {
    // TODO: heartbeat thread to shutdown/close resources if no activity after x seconds?
    this.table = tablePool.getTable(occurrenceTableName);
    try {
      this.scanner = table.getScanner(scan);
      this.iterator = scanner.iterator();
    } catch (IOException e) {
      throw new PersistenceException("Could not read from HBase", e);
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          LOG.warn("Couldn't return table to pool - continuing with possible memory leak", e);
        }
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (scannerClosed) return false;

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
