package org.gbif.occurrence.download;

import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import org.junit.Test;

public class CompressTests {


  @Test
  public void testCompress() throws  IOException {

    try (
      FileOutputStream fileOutputStream = new FileOutputStream("download.zip");
      ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(fileOutputStream));
    ) {


      ZipEntry ze = new ZipEntry("data.txt");
      zos.putNextEntry(ze, ModalZipOutputStream.MODE.DEFAULT);
      Closer closer = Closer.create();
      //Get all the files inside the directory and creates a list of InputStreams.
      try {
        D2CombineInputStream
          //Lists all the files inside the directory 'test/downloadfile/' and add each file to the zip file
          in = closer.register(new D2CombineInputStream(Lists.transform(Lists.newArrayList(new File(CompressTests.class.getClassLoader().getResource("downloadfile/").getFile()).listFiles()), new Function<File, InputStream>() {
          @Nullable
          @Override
          public InputStream apply(@Nullable File input) {
            try {
              return new FileInputStream(input);
            } catch (IOException ex) {
              Throwables.propagate(ex);
              return null;
            }
          }
        })));
        ByteStreams.copy(in, zos);
        in.close(); // required to get the sizes
        ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
        ze.setCompressedSize(in.getCompressedLength());
        ze.setCrc(in.getCrc32());
      } finally {
        closer.close();
      }

      zos.closeEntry();
      // add an entry to compress
      zos.close();
    }
  }




}
