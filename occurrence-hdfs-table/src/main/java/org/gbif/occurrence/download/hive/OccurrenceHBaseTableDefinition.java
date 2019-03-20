package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

/**
 * This provides the definition of the HBase occurrence table, for use as a Hive table.
 */
public class OccurrenceHBaseTableDefinition {

  private static final String HBASE_KEY_MAPPING = ":key"; // mapping to the HBase row key

  /**
   * Assemble the mapping for verbatim fields.
   *
   * @return the list of fields that are used in the verbatim context
   */
  private static List<HBaseField> verbatimFields() {
    Set<Term> exclusions = Stream.of(GbifTerm.gbifID,
                                     GbifTerm.mediaType // stripped explicitly as it is handled as an array
                                    ).collect(Collectors.toSet());

    return Terms.verbatimTerms().stream().filter(t  -> !exclusions.contains(t))
      .map(OccurrenceHBaseTableDefinition::verbatimField)
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * Assemble the mapping for interpreted fields, taking note that in reality, many are mounted onto the vebatim
   * HBase columns.
   *
   * @return the list of fields that are used in the interpreted context
   */
  private static List<HBaseField> interpretedFields() {
    Set<Term> exclusions = Stream.of(GbifTerm.gbifID, // treated as a special field (primary key)
                                     GbifTerm.mediaType, // stripped explicitly as it is handled as an array
                                     GbifTerm.issue, // stripped explicitly as it is handled as an array
                                     GbifTerm.numberOfOccurrences, //used for species aggregations only
                                     //Boolean flags calculated from HBase data
                                     GbifTerm.hasCoordinate,
                                     GbifTerm.hasGeospatialIssues,
                                     GbifTerm.repatriated
                                     ).collect(Collectors.toSet());

    return Terms.interpretedTerms().stream()
            .filter(t  -> !exclusions.contains(t))
            .map(OccurrenceHBaseTableDefinition::interpretedField)
            .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * The internal fields stored in HBase which we wish to expose through Hive.  The fragment and fragment hash
   * are removed and not present.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<HBaseField> internalFields() {
    Set<GbifInternalTerm> exclusions = Stream.of(GbifInternalTerm.fragmentHash, GbifInternalTerm.fragment)
                                        .collect(Collectors.toSet());

    return Arrays.stream(GbifInternalTerm.values())
            .filter(t  -> !exclusions.contains(t))
            .map(OccurrenceHBaseTableDefinition::interpretedField)
            .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * The fields stored in HBase which represent occurrence issues.
   *
   * @return the list of issue fields that are exposed through Hive
   */
  private static List<HBaseField> issueFields() {
    return Arrays.stream(OccurrenceIssue.values())
            .map(issue -> new HBaseField(GbifTerm.issue, // repeated for all, as they become an array
                                         HiveColumns.columnFor(issue), HiveDataTypes.TYPE_INT, // always
                            Columns.OCCURRENCE_COLUMN_FAMILY + ":" + Columns.column(issue)))
            .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * The fields stored in HBase which represent an extension.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<HBaseField> extensions() {
    // only MULTIMEDIA is supported
    return Collections.unmodifiableList(
      Collections.singletonList(
        new HBaseField(GbifTerm.Multimedia, HiveColumns.columnFor(Extension.MULTIMEDIA), HiveDataTypes.TYPE_STRING,
                       // always, as it has a custom serialization
           Columns.OCCURRENCE_COLUMN_FAMILY + ':' + Columns.column(Extension.MULTIMEDIA))));
  }

  /**
   * Constructs the field for the primary key, which is a special case in that it needs a special mapping.
   */
  private static HBaseField keyField() {
    return new HBaseField(GbifTerm.gbifID,
                          HiveColumns.columnFor(GbifTerm.gbifID),
                          HiveDataTypes.typeForTerm(GbifTerm.gbifID, true),
                          HBASE_KEY_MAPPING
                          // special(!) mapping just for key
    );
  }

  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields, with the types.
   */
  public static List<HBaseField> definition() {
    return ImmutableList.<HBaseField>builder()
      .add(keyField())
      .addAll(verbatimFields())
      .addAll(internalFields())
      .addAll(interpretedFields())
      .addAll(issueFields())
      .addAll(extensions())
      .build();
  }

  /**
   * Constructs a Field for the given term, when used in the verbatim context.
   */
  private static HBaseField verbatimField(Term term) {
    return new HBaseField(term,
                          HiveColumns.VERBATIM_COL_PREFIX + term.simpleName().toLowerCase(),
                          // no escape needed, due to prefix
                          HiveDataTypes.typeForTerm(term, true),
                          // verbatim context
                          Columns.OCCURRENCE_COLUMN_FAMILY + ':' + Columns.verbatimColumn(term));
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context context.
   */
  private static HBaseField interpretedField(Term term) {
    return new HBaseField(term, HiveColumns.columnFor(term),
                          // note that Columns takes care of whether this is mounted on a verbatim or an interpreted
                          // column in HBase for us
                          HiveDataTypes.typeForTerm(term, false), // not verbatim context
                          Columns.OCCURRENCE_COLUMN_FAMILY + ':' + Columns.column(term));
  }

  /**
   * Hidden constructor.
   */
  private OccurrenceHBaseTableDefinition() {
    //empty constructor
  }
}
