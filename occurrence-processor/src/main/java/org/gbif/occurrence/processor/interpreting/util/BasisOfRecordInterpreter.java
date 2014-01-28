package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.model.common.InterpretedEnum;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.basisofrecord.InterpretedBasisOfRecordParser;
import org.gbif.occurrence.processor.interpreting.result.InterpretationResult;

import com.google.common.base.Strings;

/**
 * Interpret basis of records and return controlled values.
 */
public class BasisOfRecordInterpreter {

  private static final InterpretedBasisOfRecordParser PARSER = InterpretedBasisOfRecordParser.getInstance();

  private BasisOfRecordInterpreter() {
  }

  /**
   * Compares given string to controlled vocabulary and then looks up a corresponding int value for that term.
   *
   * @param basisOfRecord string to be interpreted
   *
   * @return integer value corresponding to matching basis of record from controlled vocabulary
   */
  public static InterpretationResult<BasisOfRecord> interpretBasisOfRecord(String basisOfRecord) {
    if (!Strings.isNullOrEmpty(basisOfRecord)) {
      ParseResult<InterpretedEnum<String,BasisOfRecord>> parseResult = PARSER.parse(basisOfRecord);

      if (parseResult.isSuccessful()) {
        return new InterpretationResult<BasisOfRecord>(parseResult.getPayload().getInterpreted());
      }
    }
    return InterpretationResult.fail();
  }
}
