/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;

/**
 * A field within the definition encapsulates the information linking the Hive field, the Term in the
 * enumeration and the type for the Hive table.
 */
@Immutable
public class Field {

  private final String hiveField;
  private final String hiveDataType;
  private final Term term;

  public Field(Term term, String hiveField, String hiveDataType) {
    this.hiveField = hiveField;
    this.hiveDataType = hiveDataType;
    this.term = term;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hiveField, hiveDataType, term);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Field that = (Field) o;

    return Objects.equal(this.hiveField, that.hiveField) &&
           Objects.equal(this.hiveDataType, that.hiveDataType) &&
           Objects.equal(this.term, that.term);
  }

  @Override
  public String toString() {
    return toStringHelper().add("hiveField", hiveField).add("hiveDataType", hiveDataType).add("term", term).toString();
  }

  public String getHiveField() {
    return hiveField;
  }

  public String getHiveDataType() {
    return hiveDataType;
  }

  public Term getTerm() {
    return term;
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this).omitNullValues();
  }
}
