package org.gbif.occurrence.download.util;

import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.ArchiveField;
import org.gbif.occurrence.download.util.DwcArchiveUtils;
import org.gbif.utils.file.FileUtils;

import java.io.File;

import com.google.common.base.Strings;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class DwcArchiveUtilsTest {

  @Test
  public void testBuildDescriptor() throws Exception {
    final File archiveDir = FileUtils.createTempDir();
    System.out.println("Writing test meta.xml to " + archiveDir.toString());
    DwcArchiveUtils.createArchiveDescriptor(archiveDir);

    // read archive again
    Archive a2 = ArchiveFactory.openArchive(archiveDir);
    System.out.println(a2.getLocation().getAbsolutePath());
    for (ArchiveField af : a2.getCore().getFieldsSorted()) {
      assertNotNull("No term mapped", af.getTerm());
      assertNotNull("Missing simple term name " + af.getTerm(), Strings.emptyToNull(af.getTerm().simpleName()));
      System.out.println(af.getTerm());
    }

  }
}
