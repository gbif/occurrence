package org.gbif.occurrence.download.hive;

import org.apache.commons.io.output.StringBuilderWriter;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.vocabulary.Extension;
import org.junit.Test;

import java.io.BufferedWriter;

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
      Extension.availableExtensions());

    Download download = new Download();
    download.setKey("extension-test");
    download.setRequest(pdr);

    extensionsQuery.generateExtensionsQueryHQL(download);

    System.out.println(writer);
  }
}
