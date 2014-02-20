package org.gbif.occurrence.download.util;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

/**
 * Utility class that generates a headers file for occurrence downloads.
 */
public class HeadersFileUtil {

  public static final String DEFAULT_VERBATIM_FILE_NAME = "verbatim_headers.txt";
  public static final String DEFAULT_INTERPRETED_FILE_NAME = "interpreted_headers.txt";

  public static final String HEADERS_FILE_PATH = "inc/";

  private static final Joiner TAB_JOINER = Joiner.on('\t').skipNulls();

  /**
   * Empty private constructor.
   */
  private HeadersFileUtil() {
    // empty constructor
  }

  /**
   * Creates the headers file.
   * The output file name can be specified as argument.
   * If the file names are not specified the files are generated in
   * the current directory with the name "verbatium_headers.txt" and "verbatium_headers.txt".
   */
  public static void generateHeadersFiles(String verbatimFileName, String interpretedFileName) throws IOException {
    generateFileHeader(verbatimFileName, DEFAULT_VERBATIM_FILE_NAME, getVerbatimTableHeader());
    generateFileHeader(interpretedFileName, DEFAULT_INTERPRETED_FILE_NAME, getIntepretedTableHeader());
  }

  /**
   * Utility method that generates a file that contains the header string.
   */
  private static void generateFileHeader(String fileName, String defaultName, String header) throws IOException {
    Closer closer = Closer.create();
    final String outFileName =
      Strings.isNullOrEmpty(fileName) ? HEADERS_FILE_PATH + defaultName : fileName;
    try {
      FileWriter fileWriter = closer.register(new FileWriter(new File(outFileName)));
      fileWriter.write(header);
    } finally {
      closer.close();
    }
  }

  /**
   * Utility method that generates a String that contains the column names of terms.
   */
  private static String getTableHeader(Iterable<? extends Term> terms) {
    List<String> headers = Lists.newArrayList();
    for (Term term : terms) {
      headers.add(TermUtils.getHiveColumn(term));
    }
    return TAB_JOINER.join(headers);
  }

  /**
   * Returns the headers names of download columns.
   */
  public static String getVerbatimTableHeader() {
    return getTableHeader(TermUtils.verbatimTerms());
  }

  /**
   * Returns the headers names of download columns.
   */
  public static String getIntepretedTableHeader() {
    return getTableHeader(TermUtils.interpretedTerms());
  }


  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("2 Parameters are required: verbatim and interpreted header file names");
    }
    generateHeadersFiles(args[0], args[1]);
  }

}
