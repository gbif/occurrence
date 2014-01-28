package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.model.common.InterpretedEnum;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.countryname.InterpretedCountryParser;
import org.gbif.occurrence.processor.interpreting.result.InterpretationResult;

import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

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
    Set<OccurrenceIssue> issues = Sets.newHashSet();

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
                issues.add(OccurrenceIssue.COUNTRY_MISMATCH);
              }
            }
          } else {
            issues.add(OccurrenceIssue.COUNTRY_INVALID);
          }
        }
      }
    }

    InterpretationResult<Country> result = new InterpretationResult<Country>(c);
    // copy issues
    result.getIssues().addAll(issues);

    return result;
  }
}
