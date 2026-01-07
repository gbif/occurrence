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

import org.gbif.api.model.Constants;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.vocabulary.Extension;

import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.output.StringBuilderWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExtensionsQueryTest {

  /**
   * Checks (99%) that the columns in the SQL template are in the correct order.
   * Possibly redundant with the test in ExtensionTableTest, but this has
   * caused too many bugs before.
   */
  @Test
  public void checkColumnOrder() throws Exception {
    for (Extension extension : Extension.availableExtensions()) {
      PredicateDownloadRequest pdr = new PredicateDownloadRequest(
        new EqualsPredicate<>(OccurrenceSearchParameter.KINGDOM_KEY, "1", false),
        "MattBlissett",
        null,
        false,
        DownloadFormat.DWCA,
        DownloadType.OCCURRENCE,
        null,
        null,
        Set.of(extension),
        Constants.NUB_DATASET_KEY.toString()
      );

      Download download = new Download();
      download.setKey("extension-test");
      download.setRequest(pdr);

      StringBuilderWriter writer = new StringBuilderWriter();
      new ExtensionsQuery(new BufferedWriter(writer)).generateExtensionsQueryHQL(download);
      // System.out.println(writer);
      String query = writer.toString();

      Pattern createColumnsRX = Pattern.compile("^    `?([a-z0-9_]+)`? [a-z_]+,?$");
      Pattern insertColumnsRX = Pattern.compile("^      ext\\.([a-z0-9_]+),?$");
      List<String> createColumns = new ArrayList<>();
      List<String> insertColumns = new ArrayList<>();

      query.lines().forEach(
        l -> {
          Matcher createColumMatches = createColumnsRX.matcher(l);
          if (createColumMatches.matches()) {
            createColumns.add(createColumMatches.group(1));
          }

          Matcher insertColumMatches = insertColumnsRX.matcher(l);
          if (insertColumMatches.matches()) {
            insertColumns.add(insertColumMatches.group(1).replace("v_", ""));
          }
        }
      );

      Assertions.assertEquals(createColumns.size(), insertColumns.size());
      Assertions.assertArrayEquals(createColumns.toArray(), insertColumns.toArray(), "Columns are in mismatched order");
    }
  }
}
