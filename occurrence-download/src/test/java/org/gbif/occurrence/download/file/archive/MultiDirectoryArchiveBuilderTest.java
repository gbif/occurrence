package org.gbif.occurrence.download.file.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Test;

import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.utils.file.FileUtils;
import org.gbif.utils.file.InputStreamUtils;

import java.net.URI;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiDirectoryArchiveBuilderTest {

  @Test
  public void testBuildDefaultMode() throws Exception {
    FileSystem sourceFileSystem = new LocalFileSystem();
    sourceFileSystem.initialize(URI.create("file:///"), new Configuration());

    String[] arguments = {
      /* Resulting directory should be "first.csv" containing:
       * 000000: col1,col2,col3
       * 000001: a,b,c
       */
      FileUtils.getClasspathFile("multitsv/default/first").getAbsolutePath(),
      "first.csv",
      "col1,col2,col3",

      /* Resulting directory should be "second.csv" containing:
       * 000000: a,b,c
       * 000001: г,д,е
       */
      FileUtils.getClasspathFile("multitsv/default/second").getAbsolutePath(),
      "second.csv",
      "",

      /* Resulting directory should be "third.csv" containing:
       * 000000: colⅠ,colⅡ,colⅢ
       * 000001: a,b,c
       * 000002: г,д,е
       * 000003: η,θ,ι
       */
      FileUtils.getClasspathFile("multitsv/default/third").getAbsolutePath(),
      "third.csv",
      "colⅠ,colⅡ,colⅢ",

      /* Resulting directory should be "empty.csv" containing:
       * 000000: col一,col二,col三
       */
      FileUtils.createTempDir().getAbsolutePath(),
      "empty.csv",
      "col一,col二,col三"
    };
    String targetPath = Files.createTempDirectory("multidir-default").toString();
    String downloadKey = "testArchive";

    MultiDirectoryArchiveBuilder.withEntries(arguments)
      .mergeAllToZip(sourceFileSystem, sourceFileSystem, targetPath, downloadKey,
        ModalZipOutputStream.MODE.DEFAULT);

    ZipFile zf = new ZipFile(targetPath + "/testArchive.zip");
    ZipEntry ze = zf.getEntry("third.csv/000003");
    assertEquals("η,θ,ι\n", new InputStreamUtils().readEntireStream(zf.getInputStream(ze)));
  }

  @Test
  public void testBuildPreDeflatedMode() throws Exception {
    FileSystem sourceFileSystem = new LocalFileSystem();
    sourceFileSystem.initialize(URI.create("file:///"), new Configuration());

    // The pre-deflated files are generated with
    // D2Utils.compress(new ByteArrayInputStream("a,b,c\n".getBytes(StandardCharsets.UTF_8)),
    //   new FileOutputStream(new File("multitsv/pre_deflated/first/abc.def2")));
    // etc.

    String[] arguments = {
      /* Resulting directory should be "first.csv" containing:
       * 000000: col1,col2,col3
       * 000001: a,b,c
       */
      FileUtils.getClasspathFile("multitsv/pre_deflated/first").getAbsolutePath(),
      "first.csv",
      "col1,col2,col3",

      /* Resulting directory should be "second.csv" containing:
       * 000000: a,b,c
       * 000001: г,д,е
       */
      FileUtils.getClasspathFile("multitsv/pre_deflated/second").getAbsolutePath(),
      "second.csv",
      "",

      /* Resulting directory should be "third.csv" containing:
       * 000000: colⅠ,colⅡ,colⅢ
       * 000001: a,b,c
       * 000002: г,д,е
       * 000003: η,θ,ι
       */
      FileUtils.getClasspathFile("multitsv/pre_deflated/third").getAbsolutePath(),
      "third.csv",
      "colⅠ,colⅡ,colⅢ",

      /* Resulting directory should be "empty.csv" containing:
       * 000000: col一,col二,col三
       */
      FileUtils.createTempDir().getAbsolutePath(),
      "empty.csv",
      "col一,col二,col三"
    };
    String targetPath = Files.createTempDirectory("multidir-predeflate").toString();
    String downloadKey = "testArchive";

    MultiDirectoryArchiveBuilder.withEntries(arguments)
      .mergeAllToZip(sourceFileSystem, sourceFileSystem, targetPath, downloadKey,
        ModalZipOutputStream.MODE.PRE_DEFLATED);

    ZipFile zf = new ZipFile(targetPath + "/testArchive.zip");
    ZipEntry ze = zf.getEntry("third.csv/000003");
    assertEquals("η,θ,ι\n", new InputStreamUtils().readEntireStream(zf.getInputStream(ze)));
  }
}
