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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;

/**
 * A field within the definition encapsulates the information linking the Hive field, the Term in the
 * enumeration and the type for the Hive table.
 */
@Immutable
@EqualsAndHashCode
@AllArgsConstructor
@Getter
@Generated
public class Field {

  private final String hiveField;
  private final String hiveDataType;
  private final Term term;

}
