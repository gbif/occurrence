package org.gbif.occurrence.interpreters;

import org.gbif.api.model.common.InterpretedEnum;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceValidationRule;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.countryname.InterpretedCountryParser;
import org.gbif.occurrence.interpreters.result.InterpretationResult;

import com.google.common.base.Strings;

/**
 * Attempts to convert given country strings to the country enumeration and checks if they both match up.
 */
public class CountryInterpreter {

  private static final InterpretedCountryParser PARSER = InterpretedCountryParser.getInstance();

  private CountryInterpreter() {
  }

  /**
   * Attempts to convert given country strings to a single country, verifying the all interpreted countries
   * do not contradict.
   *
   * @param country verbatim country strings, e.g. dwc:country or dwc:countryCode
   */
  public static InterpretationResult<Country> interpretCountry(String ... country) {
    Country c = null;
    boolean mismatch = false;
    boolean invalid = false;

    if (country != null) {
      for (String verbatim : country) {
        if (!Strings.isNullOrEmpty(verbatim)) {
          ParseResult<InterpretedEnum<String,Country>> parseResult = PARSER.parse(verbatim);
          if (parseResult.isSuccessful()) {
            if (c == null) {
              c = parseResult.getPayload().getInterpreted();
            } else {
              Country c2 = parseResult.getPayload().getInterpreted();
              if (!c.equals(c2)) {
                mismatch = true;
              }
            }
          } else {
            invalid = true;
          }
        }
      }
    }

    InterpretationResult<Country> result = new InterpretationResult<Country>(c);
    // set validation rules
    result.setValidationRule(OccurrenceValidationRule.COUNTRY_MISMATCH, mismatch);
    result.setValidationRule(OccurrenceValidationRule.COUNTRY_INVALID, invalid);

    return result;
  }
}
