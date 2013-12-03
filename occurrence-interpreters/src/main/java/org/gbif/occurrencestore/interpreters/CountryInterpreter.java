package org.gbif.occurrencestore.interpreters;

import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.countryname.CountryNameParser;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Attempts to convert a given country string to a 2-letter ISO code.
 */
public class CountryInterpreter {

  private static final CountryNameParser PARSER = CountryNameParser.getInstance();
  private static final List<String> ISO_COUNTRIES = Arrays.asList(Locale.getISOCountries());

  private CountryInterpreter() {
  }

  /**
   * Attempts to convert a given country string to a 2-letter ISO code.
   *
   * @param country verbatim country
   *
   * @return 2 letter ISO country code or null if it couldn't be determined
   */
  public static String interpretCountry(String country) {
    if (country == null) {
      return null;
    }

    String isoCountry = null;
    ParseResult<String> parseResult = PARSER.parse(country.toString());
    if (parseResult.isSuccessful()) {
      isoCountry = parseResult.getPayload();
    } else if (ISO_COUNTRIES.contains(country.toString().toUpperCase())) {
      isoCountry = country.toString().toUpperCase();
    }

    return isoCountry;
  }
}
