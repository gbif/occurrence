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
package org.gbif.occurrence.download.file.dwca;

/**
 * Tables suffixes used to name tables and files.
 */
public final class TableSuffixes {

  //Suffixes for table names
  public static final String INTERPRETED_SUFFIX = "_interpreted";
  public static final String VERBATIM_SUFFIX = "_verbatim";
  public static final String MULTIMEDIA_SUFFIX = "_multimedia";
  public static final String CITATION_SUFFIX = "_citation";

  /**
   * Hidden/private constructor.
   */
  private TableSuffixes() {
    //empty constructor
  }

}
