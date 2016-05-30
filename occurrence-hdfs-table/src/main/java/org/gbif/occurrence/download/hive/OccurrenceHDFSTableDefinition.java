package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * This provides the definition required to construct the occurrence hdfs table, for use as a Hive table.
 * The table is populated by a query which scans the HBase backed table, but along the way converts some fields to
 * e.g. Hive arrays which requires some UDF voodoo captured here.
 * <p/>
 * Note to developers: It is not easy to find a perfectly clean solution to this work.  Here we try and favour long
 * term code management over all else.  For that reason, Functional programming idioms are not used even though they
 * would reduce lines of code.  However, they come at a cost in that there are several levels of method chaining
 * here, and it becomes difficult to follow as they are not so intuitive on first glance.  Similarly, we don't attempt
 * to push all complexities into the freemarker templates (e.g. complex UDF chaining) as it becomes unmanageable.
 * <p/>
 * Developers please adhere to the above design goals when modifying this class, and consider developing for simple
 * maintenance.
 */
public class OccurrenceHDFSTableDefinition {

  /**
   * Private constructor.
   */
  private OccurrenceHDFSTableDefinition() {
    //hidden constructor
  }

  /**
   * Assemble the mapping for verbatim fields.
   *
   * @return the list of fields that are used in the verbatim context
   */
  private static List<InitializableField> verbatimFields() {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Term t : DownloadTerms.DOWNLOAD_VERBATIM_TERMS) {
      builder.add(verbatimField(t));
    }
    return builder.build();
  }

  /**
   * @return a string for constructing the media type field using a UDF
   */
  private static String mediaTypeInitializer() {
    // collects from the extension multimedia term important(!)
    return "collectMediaTypes(" + HiveColumns.columnFor(Extension.MULTIMEDIA) + ")";
  }

  /**
   * @return a string for constructing the hasCoordinate field
   */
  private static String hasCoordinateInitializer() {
    return "("
           + HiveColumns.columnFor(DwcTerm.decimalLatitude)
           + " IS NOT NULL AND "
           + HiveColumns.columnFor(DwcTerm.decimalLongitude)
           + " IS NOT NULL)";
  }


  /**
   * @return a string for constructing the repatriated field
   */
  private static String repatriatedInitializer() {
    return "IF("
           + HiveColumns.columnFor(GbifTerm.publishingCountry)
           + " IS NOT NULL AND "
           + HiveColumns.columnFor(DwcTerm.countryCode)
           + " IS NOT NULL, countrycode = publishingcountry, NULL )";
  }

  private static String cleanDelimitersInitializer(String column) {
    return "cleanDelimiters(" + column + ") AS " + column;
  }

  /**
   * @return a string for constructing the hasGeospatialIssues field
   */
  private static String hasGeospatialIssuesInitializer() {

    // example output:
    //   (COALESCE(zero_coordinate,0) + COALESCE(country_coordinate_mismatch,0)) > 0

    StringBuilder statement = new StringBuilder("(");
    for (int i = 0; i < OccurrenceIssue.GEOSPATIAL_RULES.size(); i++) {
      OccurrenceIssue issue = OccurrenceIssue.GEOSPATIAL_RULES.get(i);
      statement.append("COALESCE(").append(HiveColumns.columnFor(issue)).append(",0)");
      if (i + 1 < OccurrenceIssue.GEOSPATIAL_RULES.size()) {
        statement.append(" + ");
      }
    }
    statement.append(") > 0");
    return statement.toString();
  }

  /**
   * @return a complex string which turns individual issues into a Hive array, formatted nicely for the freemarker to
   * aid debugging.
   */
  private static String issueInitializer() {
    StringBuilder statement = new StringBuilder("removeNulls(\n").append("    array(\n");
    for (int i = 0; i < OccurrenceIssue.values().length; i++) {
      OccurrenceIssue issue = OccurrenceIssue.values()[i];
      // example:  "IF(zero_coordinate IS NOT NULL, 'ZERO_COORDINATE', NULL),"
      statement.append("      IF(")
        .append(HiveColumns.columnFor(issue))
        .append(" IS NOT NULL, '")
        .append(issue.toString().toUpperCase())
        .append("', NULL)");

      if (i + 1 < OccurrenceIssue.values().length) {
        statement.append(",\n");
      } else {
        statement.append("\n");
      }
    }
    statement.append("    )\n");
    statement.append("  )");
    return statement.toString();
  }

  /**
   * Assemble the mapping for interpreted fields, taking note that in reality, many are mounted onto the verbatim
   * HBase columns.
   *
   * @return the list of fields that are used in the interpreted context
   */
  private static List<InitializableField> interpretedFields() {

    // the following terms are manipulated when transposing from HBase to hive by using UDFs and custom HQL
    Map<Term, String> initializers = ImmutableMap.<Term, String>of(GbifTerm.hasGeospatialIssues,
                                                                   hasGeospatialIssuesInitializer(),
                                                                   GbifTerm.hasCoordinate,
                                                                   hasCoordinateInitializer(),
                                                                   GbifTerm.issue,
                                                                   issueInitializer(),
                                                                   GbifTerm.repatriated,
                                                                   repatriatedInitializer());

    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (Term t : DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS) {
      // if there is custom handling registered for the term, use it
      if (initializers.containsKey(t)) {
        builder.add(interpretedField(t, initializers.get(t)));
      } else {
        builder.add(interpretedField(t)); // will just use the term name as usual
      }

    }
    return builder.build();
  }

  /**
   * The internal fields stored in HBase which we wish to expose through Hive.  The fragment and fragment hash
   * are removed and not present.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> internalFields() {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (GbifInternalTerm t : GbifInternalTerm.values()) {
      if (!DownloadTerms.EXCLUSIONS.contains(t)) {
        builder.add(interpretedField(t));
      }
    }
    return builder.build();
  }

  /**
   * The fields stored in HBase which represent an extension.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> extensions() {
    // only MULTIMEDIA is supported, but coded for future use
    Set<Extension> extensions = ImmutableSet.of(Extension.MULTIMEDIA);

    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    builder.add(interpretedField(GbifTerm.mediaType, mediaTypeInitializer()));
    for (Extension e : extensions) {
      builder.add(new InitializableField(GbifTerm.Multimedia,
                                         HiveColumns.columnFor(e),
                                         HiveDataTypes.TYPE_STRING
                                         // always, as it has a custom serialization
                  ));
    }
    return builder.build();
  }

  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields, with the types.
   */
  public static List<InitializableField> definition() {
    return ImmutableList.<InitializableField>builder()
      .add(keyField())
      .addAll(verbatimFields())
      .addAll(internalFields())
      .addAll(interpretedFields())
      .addAll(extensions())
      .build();
  }

  /**
   * Constructs the field for the primary key, which is a special case in that it needs a special mapping.
   */
  private static InitializableField keyField() {
    return new InitializableField(GbifTerm.gbifID,
                                  HiveColumns.columnFor(GbifTerm.gbifID),
                                  HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)
                                  // verbatim context
    );
  }

  /**
   * Constructs a Field for the given term, when used in the verbatim context.
   */
  private static InitializableField verbatimField(Term term) {
    String column = HiveColumns.VERBATIM_COL_PREFIX + term.simpleName().toLowerCase();
    return new InitializableField(term, column,
                                  // no escape needed, due to prefix
                                  HiveDataTypes.typeForTerm(term, true), // verbatim context
                                  cleanDelimitersInitializer(column) //remove delimiters '\n', '\t', etc.
    );
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context context constructed with no custom
   * initializer.
   */
  private static InitializableField interpretedField(Term term) {
    if (HiveDataTypes.TYPE_STRING.equals(HiveDataTypes.typeForTerm(term, false))) {
      return interpretedField(term, cleanDelimitersInitializer(HiveColumns.columnFor(term))); // no initializer
    }
    return interpretedField(term, null); // no initializer
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context context, and setting it up with the
   * given initializer.
   */
  private static InitializableField interpretedField(Term term, String initializer) {
    return new InitializableField(term,
                                  HiveColumns.columnFor(term),
                                  // note that Columns takes care of whether this is mounted on a verbatim or an interpreted
                                  // column uin HBase for us
                                  HiveDataTypes.typeForTerm(term, false),
                                  // not verbatim context
                                  initializer);
  }
}
