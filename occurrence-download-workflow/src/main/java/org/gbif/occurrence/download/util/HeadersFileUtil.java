package org.gbif.occurrence.download.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;

/**
 * Utility class that generates a headers file for occurrence downloads.
 */
public class HeadersFileUtil {

  public static final String DEFAULT_FILE_NAME = "headers.txt";

  public static final String HEADERS_FILE_PATH = "inc/" + DEFAULT_FILE_NAME;

  public static final String OCCURRENCE_HEADER = Joiner.on('\t').join(getDownloadHeaders()) + '\n';

  /**
   * Empty private constructor.
   */
  private HeadersFileUtil() {
    // empty constructor
  }

  /**
   * Appends the occurrence headers line to the output file.
   */
  public static void appendHeaders(OutputStream fileWriter) throws IOException {
    Closer resultCloser = Closer.create();
    try {
      InputStream headerInputStream =
        resultCloser.register(new ByteArrayInputStream(OCCURRENCE_HEADER.getBytes(Charsets.UTF_8)));
      ByteStreams.copy(headerInputStream, fileWriter);
    } finally {
      resultCloser.close();
    }
  }

  /**
   * Creates the headers file.
   * The output file name can be specified as argument. If the file name is not specified the file is generated in the
   * current directory with the name "headers.txt".
   */
  public static void generateHeadersFile(String outFileName) throws IOException {
    Closer closer = Closer.create();
    String fileName = Strings.isNullOrEmpty(outFileName) ? DEFAULT_FILE_NAME : outFileName;
    try {
      FileWriter fileWriter = closer.register(new FileWriter(new File(fileName)));
      fileWriter.write(OCCURRENCE_HEADER);
    } finally {
      closer.close();
    }
  }

  /**
   * Returns the headers names of download columns.
   */
  public static String[] getColumnHeaders() {
    List<String> headers = Lists.newArrayList();
//    for (FieldName field : DOWNLOAD_COLUMNS) {
//      headers.add(getHiveField(field));
//    }
    return headers.toArray(new String[headers.size()]);
  }

  /**
   * Returns the headers names of download columns.
   */
  public static String[] getDownloadHeaders() {
    List<String> headers = Lists.newArrayList();
//    for (FieldName field : DOWNLOAD_COLUMNS) {
//      String col = getHiveField(field);
//      if (col.endsWith("_")) {
//        col = col.substring(0, col.length() - 1);
//      }
//      headers.add(col);
//    }
    return headers.toArray(new String[headers.size()]);
  }

}
