package org.gbif.occurrence.download.file.dwca;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


public class DwcArchiveUtilsTest {

  /**
   * Just test that the meta.xml is created without exceptions.
   */
  @Test
  public void testCreateArchiveDescriptor(@TempDir Path testFolder) {
    DwcArchiveUtils.createArchiveDescriptor(testFolder.toFile());
  }
}
