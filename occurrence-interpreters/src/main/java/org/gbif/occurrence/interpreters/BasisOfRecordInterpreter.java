package org.gbif.occurrence.interpreters;

import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.basisofrecord.BasisOfRecordParser;
import org.gbif.common.parsers.utils.MappingUtils;

/**
 * Interpret basis of records and return controlled values.
 * TODO: return a java enum rather than int
 */
public class BasisOfRecordInterpreter {

  private static final Integer DEFAULT_RESULT = 0;
  private static final BasisOfRecordParser PARSER = BasisOfRecordParser.getInstance();

  private BasisOfRecordInterpreter() {
  }

  /**
   * Compares given string to controlled vocabulary and then looks up a corresponding int value for that term.
   *
   * @param basisOfRecord string to be interpreted
   *
   * @return integer value corresponding to matching basis of record from controlled vocabulary
   */
  public static Integer interpretBasisOfRecord(String basisOfRecord) {
    if (basisOfRecord == null) {
      return DEFAULT_RESULT;
    }

    ParseResult<String> parseResult = PARSER.parse(basisOfRecord.toString());
    if (!parseResult.isSuccessful()) {
      return DEFAULT_RESULT;
    }

    Integer result = DEFAULT_RESULT;
    Integer id = MappingUtils.mapBasisOfRecord(parseResult.getPayload());
    if (id != null) {
      result = id;
    }

    return result;
  }
}
