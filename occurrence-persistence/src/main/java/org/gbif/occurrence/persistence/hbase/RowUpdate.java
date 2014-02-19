package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.Term;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Convenient update of a single hbase row wrapping a put and a delete internally.
 * Type conversions are handled in the many overloaded setField methods.
 *
 * Setting null values will add to the internal delete.
 *
 * Use setInterpreted for all internal and gbif terms, there should be no verbatim version!
 */
public class RowUpdate {
  private static final byte[] CF = Bytes.toBytes(TableConstants.OCCURRENCE_COLUMN_FAMILY);

  private final byte[] key;
  private final Put put;
  private final Delete del;

  /**
   * Creates a new instance with an hbase put and delete.
   * @param key the row key
   */
  public RowUpdate(int key) {
    this.key = Bytes.toBytes(key);
    put = new Put(this.key);
    del = new Delete(this.key);
  }

  public byte[] getKey() {
    return key;
  }

  /**
   * Executes the put and delete on a given hbase table, finally flushing the commit.
   * @param table
   * @throws IOException
   */
  public void execute(HTableInterface table) throws IOException {
    table.put(put);
    if (!del.isEmpty()) {
      table.delete(del);
    }
    table.flushCommits();
  }

  public void setField(String column, @Nullable byte[] value) {
    if (value != null) {
      put.add(CF, Bytes.toBytes(column), value);
    } else {
      del.deleteColumn(CF, Bytes.toBytes(column));
    }
  }

  public void deleteField(String column) {
    del.deleteColumn(CF, Bytes.toBytes(column));
  }

  public void deleteField(byte [] columnQualifier) {
    del.deleteColumn(CF, columnQualifier);
  }

  public void deleteVerbatimField(Term term) {
    setField(ColumnUtil.getVerbatimColumn(term), null);
  }

  public void deleteInterpretedField(Term term) {
    setField(ColumnUtil.getColumn(term), null);
  }

  public void setVerbatimField(Term term, @Nullable byte[] value) {
    setField(ColumnUtil.getVerbatimColumn(term), value);
  }

  public void setInterpretedField(Term term, @Nullable byte[] value) {
    setField(ColumnUtil.getColumn(term), value);
  }


  public void setVerbatimField(Term term, @Nullable String value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable String value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Long value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Long value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Double value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Double value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Integer value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Integer value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Date value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Date value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Country value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Country value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable UUID value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable UUID value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Enum value) {
    setField(ColumnUtil.getVerbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Enum value) {
    setField(ColumnUtil.getColumn(term), nullSafeBytes(value));
  }


  private byte[] nullSafeBytes(String value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  private byte[] nullSafeBytes(Double value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  private static byte[] nullSafeBytes(Integer value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  private static byte[] nullSafeBytes(Long value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  private static byte[] nullSafeBytes(Date value) {
    return value == null ? null : Bytes.toBytes(value.getTime());
  }

  private static byte[] nullSafeBytes(Country value) {
    return value == null ? null : Bytes.toBytes(value.getIso2LetterCode());
  }

  private static byte[] nullSafeBytes(Enum value) {
    return value == null ? null : Bytes.toBytes(value.name());
  }

  private static byte[] nullSafeBytes(UUID value) {
    return value == null ? null : Bytes.toBytes(value.toString());
  }
}
