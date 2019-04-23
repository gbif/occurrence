package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenient update of a single HBase row wrapping a RowMutations object internally.
 * Type conversions are handled in the many overloaded setField methods.
 * Setting null values will be translated as internal deletes.
 * Use setInterpreted for all internal and gbif terms, there should be no verbatim version!
 */
public class RowUpdate {

  private static final Logger LOG = LoggerFactory.getLogger(RowUpdate.class);

  private final byte[] key;
  private final RowMutations rowMutations;

  /**
   * Creates a new instance with an HBase RowMutations object.
   *
   * @param key the row key
   */
  public RowUpdate(long key) {
    this.key = Bytes.toBytes(key);
    rowMutations = new RowMutations(this.key);
  }

  public byte[] getKey() {
    return key;
  }

  public RowMutations getRowMutations() {
    return rowMutations;
  }

  /**
   * Executes the put and delete on a given hbase table, finally flushing the commit.
   */
  public void execute(Table table) throws IOException {
    LOG.debug("Executing [{}] mutations", rowMutations.getMutations().size());
    table.mutateRow(rowMutations);
  }

  public void setField(String column, @Nullable byte[] value) throws IOException {
    if (value != null) {
      Put put = new Put(key);
      put.addColumn(Columns.CF, Bytes.toBytes(column), value);
      rowMutations.add(put);
    } else {
      Delete del = new Delete(key);
      del.addColumn(Columns.CF, Bytes.toBytes(column));
      rowMutations.add(del);
    }
  }

  public void deleteField(String column) throws IOException {
    Delete del = new Delete(this.key);
    del.addColumn(Columns.CF, Bytes.toBytes(column));
    rowMutations.add(del);
  }

  public void deleteField(byte[] columnQualifier) throws IOException {
    Delete del = new Delete(this.key);
    del.addColumn(Columns.CF, columnQualifier);
    rowMutations.add(del);
  }

  public void deleteVerbatimField(Term term) throws IOException {
    setField(Columns.verbatimColumn(term), null);
  }

  public void deleteInterpretedField(Term term) throws IOException {
    setField(Columns.column(term), null);
  }

  public void setVerbatimField(Term term, @Nullable byte[] value) throws IOException {
    setField(Columns.verbatimColumn(term), value);
  }

  public void setInterpretedField(Term term, @Nullable byte[] value) throws IOException {
    setField(Columns.column(term), value);
  }


  public void setVerbatimField(Term term, @Nullable String value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable String value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Long value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Long value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Double value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Double value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Integer value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Integer value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Date value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Date value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable Country value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Country value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }


  public void setVerbatimField(Term term, @Nullable UUID value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable UUID value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable URI value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }

  public void setVerbatimField(Term term, @Nullable Enum<?> value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable Enum<?> value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }

  public void setVerbatimField(Term term, @Nullable BigDecimal value) throws IOException {
    setField(Columns.verbatimColumn(term), nullSafeBytes(value));
  }

  public void setInterpretedField(Term term, @Nullable BigDecimal value) throws IOException {
    setField(Columns.column(term), nullSafeBytes(value));
  }

  public void setInterpretedExtension(Extension extension, @Nullable String value) throws IOException {
    setField(Columns.column(extension), nullSafeBytes(value));
  }

  public void setVerbatimExtension(Extension extension, @Nullable String value) throws IOException {
    setField(Columns.verbatimColumn(extension), nullSafeBytes(value));
  }

  private static byte[] nullSafeBytes(String value) {
    return value == null ? null : Bytes.toBytes(value);
  }

  private static byte[] nullSafeBytes(Double value) {
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

  private static byte[] nullSafeBytes(Enum<?> value) {
    return value == null ? null : Bytes.toBytes(value.name());
  }

  private static byte[] nullSafeBytes(UUID value) {
    return value == null ? null : Bytes.toBytes(value.toString());
  }

  private static byte[] nullSafeBytes(List<UUID> value) {
    return value == null || value.isEmpty() ? null : Bytes.toBytes(value.stream().map(UUID::toString).collect(Collectors.joining(";")));
  }

  private static byte[] nullSafeBytes(URI value) {
    return value == null ? null : Bytes.toBytes(value.toString());
  }

  private static byte[] nullSafeBytes(BigDecimal value) {
    return value == null ? null : Bytes.toBytes(value);
  }
}
