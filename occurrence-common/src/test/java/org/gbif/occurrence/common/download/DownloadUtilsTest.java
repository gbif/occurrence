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
package org.gbif.occurrence.common.download;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Simple test for DownloadUtils.
 */
public class DownloadUtilsTest {

  private static final char NUL_CHAR = '\0';

  @Test
  public void testNUllChar(){
    String testStr = "test";
    assertEquals(testStr, DownloadUtils.DELIMETERS_MATCH_PATTERN.matcher(testStr + NUL_CHAR).replaceAll(""));
  }
}
