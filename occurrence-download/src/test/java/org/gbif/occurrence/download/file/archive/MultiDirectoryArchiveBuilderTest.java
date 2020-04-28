package org.gbif.occurrence.download.file.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.utils.file.FileUtils;
import org.gbif.utils.file.InputStreamUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertEquals;

public class MultiDirectoryArchiveBuilderTest {

  @Test
  public void testBuildDefaultMode() throws Exception {
    FileSystem sourceFileSystem = new LocalFileSystem();
    sourceFileSystem.initialize(URI.create("file:///"), new Configuration());

    String[] arguments = {
      FileUtils.getClasspathFile("multitsv/default/first").getAbsolutePath(),
      "first.csv",
      "col1,col2,col3",

      FileUtils.getClasspathFile("multitsv/default/second").getAbsolutePath(),
      "second.csv",
      "",

      FileUtils.getClasspathFile("multitsv/default/third").getAbsolutePath(),
      "third.csv",
      "colⅠ,colⅡ,colⅢ",

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
    D2Utils.compress(new ByteArrayInputStream("г,д,е\n".getBytes()), new FileOutputStream(new File("/tmp/def")));
    D2Utils.compress(new ByteArrayInputStream("η,θ,ι\n".getBytes()), new FileOutputStream(new File("/tmp/ghi")));

    FileSystem sourceFileSystem = new LocalFileSystem();
    sourceFileSystem.initialize(URI.create("file:///"), new Configuration());

    String[] arguments = {
      FileUtils.getClasspathFile("multitsv/pre_deflated/first").getAbsolutePath(),
      "first.csv",
      "col1,col2,col3",

      FileUtils.getClasspathFile("multitsv/pre_deflated/second").getAbsolutePath(),
      "second.csv",
      "",

      FileUtils.getClasspathFile("multitsv/pre_deflated/third").getAbsolutePath(),
      "third.csv",
      "colⅠ,colⅡ,colⅢ",

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
