package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;

import com.google.common.base.Strings;

/**
 * Attempts to convert given country strings to the country enumeration and checks if they both match up.
 */
public class CountryInterpreter {

  private static final CountryParser PARSER = CountryParser.getInstance();

  private CountryInterpreter() {
  }

  /**
   * Attempts to convert given country strings to a single country, verifying the all interpreted countries
   * do not contradict.
   *
   * @param country verbatim country strings, e.g. dwc:country or dwc:countryCode
   */
  public static OccurrenceParseResult<Country> interpretCountry(String ... country) {
    if (country == null) {
      return OccurrenceParseResult.fail();
    }

    OccurrenceParseResult<Country> result = null;
    for (String verbatim : country) {
      if (!Strings.isNullOrEmpty(verbatim)) {
        if (result == null) {
          result = new OccurrenceParseResult(PARSER.parse(verbatim));

        } else if (result.isSuccessful()) {
          ParseResult<Country> result2 = PARSER.parse(verbatim);
          if (result2.isSuccessful()) {
            // only inspect secondary parsing if its also successful
            if (!result2.getPayload().equals(result.getPayload())) {
              result.getIssues().add(OccurrenceIssue.COUNTRY_MISMATCH);
            }
          }

        } else {
          // failed before. Use new parsing and add issue
          result = new OccurrenceParseResult(PARSER.parse(verbatim));
          result.getIssues().add(OccurrenceIssue.COUNTRY_INVALID);
        }
      }
    }

    if (result == null) {
      // we got an array of null or empty countries passed in
      return OccurrenceParseResult.fail();
    }

    if (!result.isSuccessful()) {
      result.getIssues().add(OccurrenceIssue.COUNTRY_INVALID);
    }
    return result;
  }


}
