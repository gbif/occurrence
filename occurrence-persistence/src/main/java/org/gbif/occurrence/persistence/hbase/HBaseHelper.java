package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.constants.FieldName;

import java.util.Date;
import java.util.UUID;
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
   * If fieldData is null, the fieldName will be added to the passed in Delete
   *
   * @param fieldName   the occurrence field to modify
   * @param fieldData   the data to write
   * @param cf          the HBase column family for this modification
   * @param put         the put to be added to
   * @param del         the delete to be added to
   */
  public static void writeField(FieldName fieldName, @Nullable byte[] fieldData, byte[] cf,
    Put put, @Nullable Delete del) {
    checkNotNull(fieldName, "fieldName may not be null");
    checkNotNull(cf, "cf may not be null");
    checkNotNull(put, "put may not be null");
    checkNotNull(del, "del may not be null");

    if (fieldData != null) {
      put.add(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fieldName).getColumnName()), fieldData);
    } else {
      del.deleteColumn(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fieldName).getColumnName()));
    }
  }

  /**
   * A convenience method for writing a new value (or deleting an existing value) for a given VerbatimOccurrence HBase
   * Term field. The write operation is added to the passed in Put or Delete.
   *
   * @param term        the verbatim occurrence field (Term) to modify
   * @param fieldData   the data to write
   * @param deleteNulls if this is true, and fieldData is null, the fieldName will be added to the passed in Delete
   * @param cf          the HBase column family for this modification
   * @param put         the put to be added to
   * @param del         the delete to be added to
   */
  public static void writeField(Term term, @Nullable byte[] fieldData, boolean deleteNulls, byte[] cf,
    Put put, @Nullable Delete del) {
    checkNotNull(term, "term may not be null");
    checkNotNull(cf, "cf may not be null");
    checkNotNull(put, "put may not be null");
    if (deleteNulls) checkNotNull(del, "del may not be null if deleteNulls is true");

    HBaseColumn column = HBaseFieldUtil.getHBaseColumn(term);
    checkNotNull(column, "term must be a valid Term");

    if (fieldData != null) {
      put.add(cf, Bytes.toBytes(column.getColumnName()), fieldData);
    } else if (deleteNulls) {
      del.deleteColumn(cf, Bytes.toBytes(column.getColumnName()));
    }
  }

  public static byte[] nullSafeBytes(String value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  public static byte[] nullSafeBytes(Double value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  public static byte[] nullSafeBytes(Integer value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  public static byte[] nullSafeBytes(Long value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  public static byte[] nullSafeBytes(Date value) {
    return value == null ? null : Bytes.toBytes(value.getTime());
  }

  public static byte[] nullSafeBytes(Country value) {
    return value == null ? null : Bytes.toBytes(value.getIso2LetterCode());
  }

  public static byte[] nullSafeBytes(Enum value) {
    return value == null ? null : Bytes.toBytes(value.name());
  }

  public static byte[] nullSafeBytes(UUID value) {
    return value == null ? null : Bytes.toBytes(value.toString());
  }

  public static <T extends Enum<T>> T nullSafeEnum(Class<T> enumType, String name) {
    if (enumType == null || name == null) {
      return null;
    }

    return Enum.valueOf(enumType, name.trim());
  }
}
