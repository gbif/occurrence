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

import lombok.Data;
/**
 * A field encapsulates the information linking the Hive field to the Term in the enumeration, the type for the Hive table
 * and a SQL fragment that can be used to initialize the field.  This is useful for create tables as follows:
 * <code>CREATE TABLE t2(id INT)</code>
 * <code>INSERT OVERWRITE TABLE t2 SELECT customUDF(x) FROM t1</code>
 * In the above example, the customUDF(x) is part of the field definition.
 */
@Immutable
@Data
public class InitializableField {

  private final String initializer;
  private final String hiveField;
  private final String hiveDataType;
  private final Term term;

  /**
   * Default behavior is to initialize with the same as the Hive table column, implying the column is a straight copy
   * with the same name as an existing table.
   */
  public InitializableField(Term term, String hiveField, String hiveDataType) {
    this.term = term;
    this.hiveField = hiveField;
    this.hiveDataType = hiveDataType;
    initializer = hiveField;
  }

  public InitializableField(Term term, String hiveField, String hiveDataType, String initializer) {
    this.term = term;
    this.hiveField = hiveField;
    this.hiveDataType = hiveDataType;
    this.initializer = initializer == null ? hiveField : initializer; // for safety
  }
}
