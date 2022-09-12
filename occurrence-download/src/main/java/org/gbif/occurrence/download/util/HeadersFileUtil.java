/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.util;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

/**
 * Utility class that generates a headers file for occurrence downloads.
 * Header columns in general use the simple name of a term.
 */
public class HeadersFileUtil {

  public static final String DEFAULT_VERBATIM_FILE_NAME = "verbatim_headers.txt";
  public static final String DEFAULT_INTERPRETED_FILE_NAME = "interpreted_headers.txt";
  public static final String DEFAULT_MULTIMEDIA_FILE_NAME = "multimedia_headers.txt";
  public static final String HEADERS_FILE_PATH = "inc/";

  private static final Joiner TAB_JOINER = Joiner.on('\t').skipNulls();

  /**
   * Creates the headers file.
   * The output file name can be specified as argument.
   * If the file names are not specified the files are generated in
   * the current directory with the name "verbatim_headers.txt" and "verbatim_headers.txt".
   */
  public static void generateHeadersFiles(
    String verbatimFileName,
    String interpretedFileName,
    String multimediaFileName
  ) throws IOException {
    generateFileHeader(verbatimFileName, DEFAULT_VERBATIM_FILE_NAME, getVerbatimTableHeader());
    generateFileHeader(interpretedFileName, DEFAULT_INTERPRETED_FILE_NAME, getInterpretedTableHeader());
    generateFileHeader(multimediaFileName, DEFAULT_MULTIMEDIA_FILE_NAME, getMultimediaTableHeader());
  }

  /**
   * Utility method that generates a file that contains the header string.
   */
  private static void generateFileHeader(String fileName, String defaultName, String header) throws IOException {
    String outFileName = Strings.isNullOrEmpty(fileName) ? HEADERS_FILE_PATH + defaultName : fileName;
    try (FileWriter fileWriter = new FileWriter(outFileName)) {
      fileWriter.write(header);
    }
  }

  /**
   * Utility method that generates a String that contains the column names of terms.
   */
  private static String getTableHeader(Iterable<? extends Term> terms) {
    List<String> headers = Lists.newArrayList();
    for (Term term : terms) {
      headers.add(term.simpleName());
    }
    return TAB_JOINER.join(headers) + '\n';
  }

  /**
   * Appends the occurrence headers line to the output file.
   */
  public static void appendInterpretedHeaders(OutputStream fileWriter) throws IOException {
    appendHeaders(fileWriter, getInterpretedTableHeader());
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
    try (InputStream headerInputStream = new ByteArrayInputStream(headers.getBytes())) {
      ByteStreams.copy(headerInputStream, fileWriter);
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
  public static String getInterpretedTableHeader() {
    return getTableHeader(TermUtils.interpretedTerms());
  }

  /**
   * Returns the headers names of download columns.
   */
  public static String getMultimediaTableHeader() {
    return getTableHeader(TermUtils.multimediaTerms());
  }

  public static String getExtensionInterpretedHeader(OccurrenceHDFSTableDefinition.ExtensionTable extensionTable) {
    return TAB_JOINER.join(extensionTable.getInterpretedFields()) + '\n';
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      throw new IllegalArgumentException(
        "3 Parameters are required: verbatim, interpreted and multimedia header file names");
    }
    generateHeadersFiles(args[0], args[1], args[2]);
  }

  /**
   * Empty private constructor.
   */
  private HeadersFileUtil() {
    // empty constructor
  }

}
