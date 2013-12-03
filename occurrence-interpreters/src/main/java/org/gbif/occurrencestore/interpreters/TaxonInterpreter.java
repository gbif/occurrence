package org.gbif.occurrencestore.interpreters;

import org.gbif.common.parsers.utils.ClassificationUtils;
import org.gbif.common.parsers.utils.MappingUtils;

import com.google.common.base.Strings;

/**
 * Wrappers around a number of routines for cleaning up taxon names.
 *
 * @see ClassificationUtils
 * @see MappingUtils
 */
public class TaxonInterpreter {

  private TaxonInterpreter() {
  }

  public static String parseName(String name) {
    if (name == null) return null;

    String parsedName = ClassificationUtils.parseName(name.toString());
    if (Strings.isNullOrEmpty(parsedName)) {
      return null;
    }

    return parsedName;
  }

  public static String mapKingdom(String taxon) {
    if (taxon == null) return null;

    String lookup = MappingUtils.mapKingdom(taxon);
    String result;
    if (Strings.isNullOrEmpty(lookup)) {
      result = taxon;
    } else {
      result = lookup;
    }

    return result;
  }

  public static String mapPhylum(String taxon) {
    if (taxon == null) return null;

    String lookup = MappingUtils.mapPhylum(taxon);
    String result;
    if (Strings.isNullOrEmpty(lookup)) {
      result = taxon;
    } else {
      result = lookup;
    }

    return result;
  }

  public static String cleanTaxon(String taxon) {
    if (taxon == null) return null;

    String cleanedTaxon = ClassificationUtils.clean(taxon.toString());

    if (Strings.isNullOrEmpty(cleanedTaxon)) {
      return null;
    }

    return cleanedTaxon;
  }

  public static String cleanAuthor(String author) {
    if (author == null) return null;

    String cleanedAuthor = ClassificationUtils.cleanAuthor(author.toString());
    if (Strings.isNullOrEmpty(cleanedAuthor)) {
      return null;
    }

    return cleanedAuthor;
  }
}
