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

import org.gbif.api.vocabulary.Extension;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DwcArchiveUtilsTest {

  /**
   * Just test that the meta.xml is created without exceptions.
   */
  @Test
  public void testCreateArchiveDescriptor(@TempDir Path testFolder) {
    DwcArchiveUtils.createOccurrenceArchiveDescriptor(testFolder.toFile(), Collections.singleton(Extension.MEASUREMENT_OR_FACT));
  }
}
