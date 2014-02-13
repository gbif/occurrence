package org.gbif.occurrence.persistence.hbase;

import com.google.common.base.Objects;

/**
* An hbase column with its family.
*/
public class HBaseColumn {

  private final String familyName;
  private final String columnName;

  public HBaseColumn(String familyName, String columnName) {
    this.familyName = familyName;
    this.columnName = columnName;
  }

  public String getFamilyName() {
    return familyName;
  }

  public String getColumnName() {
    return columnName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(familyName, columnName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HBaseColumn)) {
      return false;
    }
    HBaseColumn that = (HBaseColumn) obj;
    return Objects.equal(this.familyName, that.familyName)
           && Objects.equal(this.columnName, that.columnName);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("family", familyName)
      .add("name", columnName)
      .toString();
  }
}
