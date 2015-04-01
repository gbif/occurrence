package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;

/**
 * A field encapsulates the information linking the Hive field the Term in the enumeration, the type for the Hive table
 * and a SQL fragment that can be used to initialize the field.  This is useful for create tables as follows:
 * <code>CREATE TABLE t2(id INT)</code>
 * <code>INSERT OVERWRITE TABLE t2 SELECT customUDF(x) FROM t1</code>
 * In the above example, the customUDF(x) is part of the field definition.
 */
@Immutable
public class InitializableField extends Field {

  private final String initializer;

  /**
   * Default behavior is to initialize with the same as the Hive table column, implying the column is a straight copy
   * with the same name as an existing table.
   */
  public InitializableField(Term term, String hiveField, String hiveDataType) {
    super(term, hiveField, hiveDataType);
    initializer = getHiveField();
  }

  public InitializableField(Term term, String hiveField, String hiveDataType, String initializer) {
    super(term, hiveField, hiveDataType);
    this.initializer = initializer == null ? getHiveField() : initializer; // for safety
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(initializer, hiveField, hiveDataType, term);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("hiveField", hiveField)
                  .add("initializer", initializer)
                  .add("hiveDataType", hiveDataType)
                  .add("term", term)
                  .toString();
  }

  public String getInitializer() {
    return initializer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InitializableField that = (InitializableField) o;

    return Objects.equal(this.initializer, that.initializer) &&
           Objects.equal(this.hiveField, that.hiveField) &&
           Objects.equal(this.hiveDataType, that.hiveDataType) &&
           Objects.equal(this.term, that.term);
  }
}
