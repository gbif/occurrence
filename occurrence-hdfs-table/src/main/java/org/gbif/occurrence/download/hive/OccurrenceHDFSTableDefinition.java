package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
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
 * The table is populated by a query which scans the Avro files, but along the way converts some fields to
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

  private static String cleanDelimitersInitializer(String column) {
    return "cleanDelimiters(" + column + ") AS " + column;
  }

  /**
   * Assemble the mapping for interpreted fields, taking note that in reality, many are mounted onto the verbatim
   * columns.
   *
   * @return the list of fields that are used in the interpreted context
   */
  private static List<InitializableField> interpretedFields() {

    // the following terms are manipulated when transposing from Avro to hive by using UDFs and custom HQL
    Map<Term, String> initializers = ImmutableMap.<Term, String>builder()
                                      .put(GbifTerm.datasetKey, HiveColumns.columnFor(GbifTerm.datasetKey))
                                      .put(GbifTerm.protocol, HiveColumns.columnFor(GbifTerm.protocol))
                                      .put(GbifTerm.publishingCountry, HiveColumns.columnFor(GbifTerm.publishingCountry))
                                      .build();

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
   * The internal fields stored in Avro which we wish to expose through Hive.  The fragment and fragment hash
   * are removed and not present.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> internalFields() {
    Map<Term, String> initializers = new ImmutableMap.Builder<Term,String>()
                                                      .put(GbifInternalTerm.publishingOrgKey, HiveColumns.columnFor(GbifInternalTerm.publishingOrgKey))
                                                      .put(GbifInternalTerm.installationKey, HiveColumns.columnFor(GbifInternalTerm.installationKey))
                                                      .put(GbifInternalTerm.projectId, HiveColumns.columnFor(GbifInternalTerm.projectId))
                                                      .put(GbifInternalTerm.programmeAcronym, HiveColumns.columnFor(GbifInternalTerm.programmeAcronym))
                                            .build();
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    for (GbifInternalTerm t : GbifInternalTerm.values()) {
      if (!DownloadTerms.EXCLUSIONS.contains(t)) {
        if (initializers.containsKey(t)) {
          builder.add(interpretedField(t, initializers.get(t)));
        } else {
          builder.add(interpretedField(t));
        }
      }
    }
    return builder.build();
  }

  /**
   * The fields stored in Avro which represent an extension.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> extensions() {
    // only MULTIMEDIA is supported, but coded for future use
    Set<Extension> extensions = ImmutableSet.of(Extension.MULTIMEDIA);
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
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
                                  // note that Columns takes care of whether this is mounted
                                  // on a verbatim or an interpreted column for us
                                  HiveDataTypes.typeForTerm(term, false),
                                  // not verbatim context
                                  initializer);
  }
}
