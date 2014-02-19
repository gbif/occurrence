package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.util.VocabularyUtils;
import org.gbif.dwc.terms.Term;
import org.gbif.hbase.util.ResultReader;

import java.util.Date;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A convenience class for making things easier when reading the fields of an HBase result from the occurrence table.
 */
public class ExtResultReader {

  private static final Logger LOG = LoggerFactory.getLogger(ExtResultReader.class);
  private static String CF = TableConstants.OCCURRENCE_COLUMN_FAMILY;
  /**
   * Should never be instantiated.
   */
  private ExtResultReader() {
  }

  public static int getKey(Result row) {
    return Bytes.toInt(row.getRow());
  }

  public static String getString(Result row, String column) {
    return getString(row, column, null);
  }

  public static String getString(Result row, Term column) {
    return getString(row, ColumnUtil.getColumn(column), null);
  }

  public static String getString(Result row, String column, @Nullable String defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    return ResultReader.getString(row, CF, column, defaultValue);
  }

  public static UUID getUuid(Result row, String column) {
    String uuid = getString(row, column);
    return uuid == null ? null : UUID.fromString(uuid);
  }

  public static UUID getUuid(Result row, Term column) {
    String uuid = getString(row, ColumnUtil.getColumn(column));
    return uuid == null ? null : UUID.fromString(uuid);
  }

  public static Double getDouble(Result row, String column) {
    return getDouble(row, column, null);
  }

  public static Double getDouble(Result row, Term column) {
    return getDouble(row, ColumnUtil.getColumn(column), null);
  }

  public static Double getDouble(Result row, String column, @Nullable Double defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    return ResultReader.getDouble(row, CF, column, defaultValue);
  }

  public static Integer getInteger(Result row, String column) {
    return getInteger(row, column, null);
  }

  public static Integer getInteger(Result row, Term column) {
    return getInteger(row, ColumnUtil.getColumn(column), null);
  }

  public static Integer getInteger(Result row, String column, @Nullable Integer defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    return ResultReader.getInteger(row, CF, column, defaultValue);
  }

  public static Date getDate(Result row, String column) {
    Long time = getLong(row, column);
    return time == null ? null : new Date(time);
  }

  public static Date getDate(Result row, Term column) {
    return getDate(row, ColumnUtil.getColumn(column));
  }

  public static Long getLong(Result row, String column) {
    return getLong(row, column, null);
  }

  public static Long getLong(Result row, Term column) {
    return getLong(row, ColumnUtil.getColumn(column), null);
  }

  public static Long getLong(Result row, String column, @Nullable Long defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    return ResultReader.getLong(row, CF, column, defaultValue);
  }

  public static byte[] getBytes(Result row, Term column) {
    return getBytes(row, ColumnUtil.getColumn(column), null);
  }

  public static byte[] getBytes(Result row, String column) {
    return getBytes(row, column, null);
  }

  public static byte[] getBytes(Result row, String column, @Nullable byte[] defaultValue) {
    checkNotNull(row, "row can't be null");
    checkNotNull(column, "column can't be null");
    return ResultReader.getBytes(row, CF, column, defaultValue);
  }

  public static <T extends Enum<?>> T getEnum(Result row, Term column, Class<T> enumClass) {
    String value = getString(row, ColumnUtil.getColumn(column), null);
    if (!Strings.isNullOrEmpty(value)) {
      try {
        return (T) VocabularyUtils.lookupEnum(value, enumClass);
      } catch (IllegalArgumentException e) {
        // value not matching enum!!! LOG???
      }
    }
    return null;
  }
}
