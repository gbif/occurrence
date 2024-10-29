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
package org.gbif.occurrence.spark.udf;

import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;
import scala.collection.mutable.WrappedArrayBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

public class StringArrayLikeGenericUdfTest {

  @Test
  public void testStringArrayLikeGenericUdf() throws Exception {
    StringArrayLikeGenericUdf function = new StringArrayLikeGenericUdf();
    WrappedArray<String> array = WrappedArray.make(new String[] {"ab\\c?d*e", "ab\\c?d*eXfXXg", "^[abc].*x{1}$"});

    // No wildcards in this pattern.  b is escaped unnecessarily.
    String notWild = "a\\b\\\\c\\?d\\*E";
    String wild = "ab\\\\c\\?d\\*E?F*G";
    String wildSens = "ab\\\\c\\?d\\*e?f*g";
    // Don't accept regular expressions.
    String notRegex = "^[abc]\\.\\*x{1}$";

    assertTrue(function.call(array, notWild, false));
    assertTrue(function.call(array, wild, false));
    assertTrue(function.call(array, wildSens, true));
    assertTrue(function.call(array, notRegex, false));
  }
}
