package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;

/**
 * A field within the definition encapsulates the information linking the Hive field, the HBase column, the Term in the
 * enumeration and the type for the Hive table.
 */
@Immutable
public class HBaseField extends Field {

  private final String hbaseColumn;

  public HBaseField(Term term, String hiveField, String hiveDataType, String hbaseColumn) {
    super(term, hiveField, hiveDataType);
    this.hbaseColumn = hbaseColumn;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hiveField, hbaseColumn, hiveDataType, term);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("hiveField", hiveField)
                  .add("hbaseColumn", hbaseColumn)
                  .add("hiveDataType", hiveDataType)
                  .add("term", term)
                  .toString();
  }

  public String getHbaseColumn() {
    return hbaseColumn;
  }
}
