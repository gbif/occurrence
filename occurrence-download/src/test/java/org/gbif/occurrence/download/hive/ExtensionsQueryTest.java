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

import org.apache.commons.io.output.StringBuilderWriter;
import org.junit.Test;

public class ExtensionsQueryTest {

  @Test
  public void manualTest() throws Exception {

    StringBuilderWriter writer = new StringBuilderWriter();

    ExtensionsQuery extensionsQuery = new ExtensionsQuery(new BufferedWriter(writer));

    PredicateDownloadRequest pdr = new PredicateDownloadRequest(
      new EqualsPredicate<>(OccurrenceSearchParameter.KINGDOM_KEY, "1", false),
      "MattBlissett",
      null,
      false,
      DownloadFormat.DWCA,
      DownloadType.OCCURRENCE,
      null,
      null,
      Extension.availableExtensions(),
      null,
      Constants.NUB_DATASET_KEY.toString()
    );

    Download download = new Download();
    download.setKey("extension-test");
    download.setRequest(pdr);

    extensionsQuery.generateExtensionsQueryHQL(download);

    System.out.println(writer);
  }
}
