package org.gbif.occurrence.persistence.hbase;

import org.gbif.occurrence.common.constants.FieldName;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper methods for working with HBase.
 */
public class HBaseHelper {

  // shouldn't be instantiated
  private HBaseHelper() {
  }

  /**
   * A convenience method for writing a new value (or deleting an existing value) for a given Occurrence HBase field.
   * The write operation is added to the passed in Put or Delete.
   *
   * @param fieldName   the occurrence field to modify
   * @param fieldData   the data to write
   * @param deleteNulls if this is true, and fieldData is null, the fieldName will be added to the passed in Delete
   * @param cf          the HBase column family for this modification
   * @param put         the put to be added to
   * @param del         the delete to be added to
   */
  public static void writeField(FieldName fieldName, @Nullable byte[] fieldData, boolean deleteNulls, byte[] cf,
    Put put, @Nullable Delete del) {
    checkNotNull(fieldName, "fieldName may not be null");
    checkNotNull(cf, "cf may not be null");
    checkNotNull(put, "put may not be null");
    if (deleteNulls) checkNotNull(del, "del may not be null if deleteNulls is true");

    if (fieldData != null) {
      put.add(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fieldName).getColumnName()), fieldData);
    } else if (deleteNulls) {
      del.deleteColumn(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fieldName).getColumnName()));
    }
  }
}
