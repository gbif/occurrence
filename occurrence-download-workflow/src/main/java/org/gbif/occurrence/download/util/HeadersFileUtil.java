package org.gbif.occurrence.download.util;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;

/**
 * Utility class that generates a headers file for occurrence downloads.
 */
public class HeadersFileUtil {

  public static final String DEFAULT_VERBATIM_FILE_NAME = "verbatim_headers.txt";
  public static final String DEFAULT_INTERPRETED_FILE_NAME = "interpreted_headers.txt";
  public static final String DEFAULT_MULTIMEDIA_FILE_NAME = "multimedia_headers.txt";
  public static final String MULTIMEDIA_ID_COLUMN = "coreid";
  public static final String HEADERS_FILE_PATH = "inc/";

  private static final Joiner TAB_JOINER = Joiner.on('\t').skipNulls();
  private static final String[] MULTIMEDIA_HEADERS = new String[] {MULTIMEDIA_ID_COLUMN, "type", "format",
    "identifier", "references", "title", "description", "source", "audience", "created", "creator", "contributor",
    "publisher", "license", "rightsHolder"};

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
  public static void
    generateHeadersFiles(String verbatimFileName, String interpretedFileName, String multimediaFileName)
      throws IOException {
    generateFileHeader(verbatimFileName, DEFAULT_VERBATIM_FILE_NAME, getVerbatimTableHeader());
    generateFileHeader(interpretedFileName, DEFAULT_INTERPRETED_FILE_NAME, getIntepretedTableHeader());
    generateFileHeader(multimediaFileName, DEFAULT_MULTIMEDIA_FILE_NAME, TAB_JOINER.join(MULTIMEDIA_HEADERS) + '\n');
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
    return TAB_JOINER.join(headers) + '\n';
  }

  /**
   * Utility method that generates an array of string that contains the column names of terms.
   */
  private static String[] getTableColumns(Iterable<? extends Term> terms) {
    List<String> headers = Lists.newArrayList();
    for (Term term : terms) {
      headers.add(TermUtils.getHiveColumn(term));
    }
    return headers.toArray(new String[headers.size()]);
  }

  /**
   * Appends the occurrence headers line to the output file.
   */
  public static void appendInterpretedHeaders(OutputStream fileWriter) throws IOException {
    appendHeaders(fileWriter, getIntepretedTableHeader());
  }

  /**
   * Appends the occurrence headers line to the output file.
   */
  public static void appendVerbatimHeaders(OutputStream fileWriter) throws IOException {
    appendHeaders(fileWriter, getVerbatimTableHeader());
  }

  /**
   * Appends the occurrence headers line to the output file.
   */
  public static void appendMultimediaHeaders(OutputStream fileWriter) throws IOException {
    appendHeaders(fileWriter, getMultimediaTableHeader());
  }


  /**
   * Appends the headers line to the output file.
   */
  private static void appendHeaders(OutputStream fileWriter, String headers) throws IOException {
    Closer resultCloser = Closer.create();
    try {
      InputStream headerInputStream =
        resultCloser.register(new ByteArrayInputStream(headers.getBytes()));
      ByteStreams.copy(headerInputStream, fileWriter);
    } finally {
      resultCloser.close();
    }
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

  /**
   * Returns the headers names of download columns.
   */
  public static String getMultimediaTableHeader() {
    return TAB_JOINER.join(MULTIMEDIA_HEADERS) + '\n';
  }


  /**
   * Returns a list column names of interpreted fields.
   */
  public static String[] getIntepretedTableColumns() {
    return getTableColumns(TermUtils.interpretedTerms());
  }

  /**
   * Returns a list column names of verbatim fields.
   */
  public static String[] getVerbatimTableColumns() {
    return getTableColumns(TermUtils.verbatimTerms());
  }

  /**
   * Returns a list column names of multimedia fields.
   */
  public static String[] getMultimediaTableColumns() {
    return MULTIMEDIA_HEADERS;
  }


  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      throw new IllegalArgumentException(
        "3 Parameters are required: verbatim, interpreted and multimedia header file names");
    }
    generateHeadersFiles(args[0], args[1], args[2]);
  }

}
