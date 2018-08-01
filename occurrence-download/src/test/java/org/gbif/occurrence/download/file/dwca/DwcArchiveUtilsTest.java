package org.gbif.occurrence.download.file.dwca;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DwcArchiveUtilsTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  /**
   * Just test that the meta.xml is created without exceptions.
   */
  @Test
  public void testCreateArchiveDescriptor() {
    DwcArchiveUtils.createArchiveDescriptor(testFolder.getRoot());
  }
}
