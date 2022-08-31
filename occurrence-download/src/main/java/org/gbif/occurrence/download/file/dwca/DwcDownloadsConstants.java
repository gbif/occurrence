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
 * Common constants used to construct the DwcA download file.
 */
public class DwcDownloadsConstants {

  public static final String METADATA_FILENAME = "metadata.xml";
  public static final String OCCURRENCE_INTERPRETED_FILENAME = "occurrence.txt";
  public static final String EVENT_INTERPRETED_FILENAME = "event.txt";
  public static final String VERBATIM_FILENAME = "verbatim.txt";
  public static final String MULTIMEDIA_FILENAME = "multimedia.txt";
  public static final String CITATIONS_FILENAME = "citations.txt";
  public static final String RIGHTS_FILENAME = "rights.txt";
  public static final String DESCRIPTOR_FILENAME = "meta.xml";
  /**
   * Hidden constructor.
   */
  private DwcDownloadsConstants() {
    // default private constructor
  }
}
