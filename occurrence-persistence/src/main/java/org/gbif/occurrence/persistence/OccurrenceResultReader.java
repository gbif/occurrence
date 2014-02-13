package org.gbif.occurrence.persistence;

import org.gbif.dwc.terms.Term;
import org.gbif.hbase.util.ResultReader;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.constants.FieldType;
import org.gbif.occurrence.persistence.hbase.HBaseColumn;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;

import java.util.Date;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A convenience class for making things easier when reading the fields of an HBase result from the occurrence table.
 */
public class OccurrenceResultReader {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResultReader.class);

  /**
   * Should never be instantiated.
   */
  private OccurrenceResultReader() {
  }

  public static int getKey(Result row) {
    return Bytes.toInt(row.getRow());
  }

  public static UUID getUuid(Result row, FieldName column) {
    String uuid = getString(row, column);
    return uuid == null ? null : UUID.fromString(uuid);
  }

  public static String getString(Result row, FieldName column) {
    return getString(row, column, null);
  }

  public static String getTermString(Result row, Term term) {
    checkNotNull(row, "row can't be null");
    checkNotNull(term, "term can't be null");

    HBaseColumn hBaseColumn = HBaseFieldUtil.getHBaseColumn(term);
    if (hBaseColumn == null) {
      return null;
    }
    return ResultReader.getString(row, hBaseColumn.getFamilyName(), hBaseColumn.getColumnName(), null);
  }

  public static String getString(Result row, FieldName column, @Nullable String defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    checkArgument(column.getType() == FieldType.STRING, "FieldName [" + column.toString() + "] is not of type String");

    HBaseColumn col = HBaseFieldUtil.getHBaseColumn(column);
    String result = ResultReader.getString(row, col.getFamilyName(), col.getColumnName(), defaultValue);
    return result;
  }

  public static Double getDouble(Result row, FieldName column) {
    return getDouble(row, column, null);
  }

  public static Double getDouble(Result row, FieldName column, @Nullable Double defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    checkArgument(column.getType() == FieldType.DOUBLE, "FieldName [" + column.toString() + "] is not of type Double");

    return ResultReader.getDouble(row, HBaseFieldUtil.getHBaseColumn(column).getFamilyName(),
      HBaseFieldUtil.getHBaseColumn(column).getColumnName(), defaultValue);
  }

  public static Integer getInteger(Result row, FieldName column) {
    return getInteger(row, column, null);
  }

  public static Integer getInteger(Result row, FieldName column, @Nullable Integer defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    checkArgument(column.getType() == FieldType.INT, "FieldName [" + column.toString() + "] is not of type Integer");

    return ResultReader.getInteger(row, HBaseFieldUtil.getHBaseColumn(column).getFamilyName(),
      HBaseFieldUtil.getHBaseColumn(column).getColumnName(), defaultValue);
  }

  public static Date getDate(Result row, FieldName column) {
    Long time = getLong(row, column);
    return time == null ? null : new Date(time);
  }

  public static Long getLong(Result row, FieldName column) {
    return getLong(row, column, null);
  }

  public static Long getLong(Result row, FieldName column, @Nullable Long defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    checkArgument(column.getType() == FieldType.LONG, "FieldName [" + column.toString() + "] is not of type Long");

    return ResultReader.getLong(row, HBaseFieldUtil.getHBaseColumn(column).getFamilyName(),
      HBaseFieldUtil.getHBaseColumn(column).getColumnName(), defaultValue);
  }

  public static byte[] getBytes(Result row, FieldName column) {
    return getBytes(row, column, null);
  }

  public static byte[] getBytes(Result row, FieldName column, @Nullable byte[] defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    checkArgument(column.getType() == FieldType.BYTES, "FieldName [" + column.toString() + "] is not of type byte[]");

    return ResultReader.getBytes(row, HBaseFieldUtil.getHBaseColumn(column).getFamilyName(),
      HBaseFieldUtil.getHBaseColumn(column).getColumnName(), defaultValue);
  }

  public static Object get(Result row, FieldName column) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    Object result = null;

    switch (column.getType()) {
      case INT:
        result = getInteger(row, column);
        break;
      case STRING:
        result = getString(row, column);
        break;
      case DOUBLE:
        result = getDouble(row, column);
        break;
      case LONG:
        result = getLong(row, column);
        break;
      case BYTES:
        result = getBytes(row, column);
        break;
      default:
        LOG.info("FieldType [{}] is not supported.", column.getType().toString());
    }

    return result;
  }
}
