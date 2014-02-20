package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.persistence.api.InternalTerm;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.util.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Utility class to deal with occurrence hbase columns.
 * Primarily translate from Terms to their corresponding HBase column name (in the occurrence table), but
 * also deals with any other names used, e.g. identifiers, issue columns, etc.
 */
public class Columns {


  // the one column family for all columns of the occurrence table
  public static final String OCCURRENCE_COLUMN_FAMILY = "o";
  public static final byte[] CF = Bytes.toBytes(OCCURRENCE_COLUMN_FAMILY);

  // a prefix required for all non term based columns
  private static final String INTERNAL_PREFIX = "_";

  // the counter table is a single cell that is the "autoincrement" number for new keys, with column family, column,
  // and key ("row" in hbase speak)
  public static final String COUNTER_COLUMN = INTERNAL_PREFIX + "id";

  // the lookup table is a secondary index of unique ids (holy triplet or publisher-provided) to GBIF integer keys
  public static final String LOOKUP_KEY_COLUMN = INTERNAL_PREFIX + "i";
  public static final String LOOKUP_LOCK_COLUMN = INTERNAL_PREFIX + "l";
  public static final String LOOKUP_STATUS_COLUMN = INTERNAL_PREFIX + "s";

  // each UnknownTerm is prefixed differently
  private static final String VERBATIM_TERM_PREFIX = "v_";

  // a single occurrence will have 0 or more OccurrenceIssues. Once column per issue, each one prefixed
  private static final String ISSUE_PREFIX = INTERNAL_PREFIX + "iss_";

  // An occurrence can have 0-n identifiers, each of a certain type. Their column names look like _t1, _i1, _t2, _i2, etc.
  private static final String IDENTIFIER_TYPE_COLUMN = INTERNAL_PREFIX + "t";
  private static final String IDENTIFIER_COLUMN = INTERNAL_PREFIX + "i";

  // bootstrap term factory, adding the new InternalTerm enum
  static {
    TermFactory tf = TermFactory.instance();
    for (InternalTerm t : InternalTerm.values()) {
      tf.addTerm(t.qualifiedName(), t);
      tf.addTerm(t.simpleName(), t);
    }
  }

  /**
   * Should never be instantiated.
   */
  private Columns() {
  }

  /**
   * Returns the column for the given term.
   * If an interpreted column exists for the given term it will be returned, otherwise the verbatim column will be used.
   * Not that InternalTerm are always interpreted and do not exist as verbatim columns.
   *
   * Asking for a "secondary" interpreted term like country which is used during interpretation but not stored
   * will result in an IllegalArgumentException. dwc:countryCode is the right term in this case.
   *
   * Key terms like taxonID or occurrenceID are considered verbatim terms and do not map to the respective GBIF columns.
   * Please use the GbifTerm enum for those!
   */
  public static String column(Term term) {
    if (term instanceof InternalTerm || TermUtils.isOccurrenceJavaProperty(term)) {
      return column(term, "");

    } else if(TermUtils.isInterpretedSourceTerm(term)) {
      // "secondary" terms used in interpretation but not used to store the interpreted values should never be asked for
      throw new IllegalArgumentException("The term " + term + " is interpreted and only relevant for verbatim values");

    } else {
      return verbatimColumn(term);
    }
  }

  /**
   * Returns the verbatim column for a term.
   * InternalTerm is not permitted and will result in an IllegalArgumentException!
   * @param term
   * @return
   */
  public static String verbatimColumn(Term term) {
    if (term instanceof InternalTerm) {
      throw new IllegalArgumentException("Internal terms do not exist as verbatim columns");
    }
    return column(term, VERBATIM_TERM_PREFIX);
  }

  private static String column(Term term, String colPrefix) {
    checkNotNull(term, "term can't be null");

    // unknown terms will never be mapped in Hive, and we can't replace : with anything and guarantee that it will
    // be reversible
    if (term instanceof UnknownTerm) {
      return colPrefix + term.qualifiedName();
    }

    // known terms are mapped to their unique simple name with an optional (v_) prefix
    return colPrefix + term.simpleName();
  }

  public static String idColumn(int index) {
    return IDENTIFIER_COLUMN + index;
  }

  public static String idTypeColumn(int index) {
    return IDENTIFIER_TYPE_COLUMN+ index;
  }

  /**
   * Returns the term for a strictly verbatim column.
   * If the column given is not a verbatim column, null will be returned.
   */
  @Nullable
  public static Term termFromVerbatimColumn(byte[] qualifier) {
    checkNotNull(qualifier, "qualifier can't be null");
    String colName = Bytes.toString(qualifier);

    if (!colName.startsWith(VERBATIM_TERM_PREFIX)) {
      // we asked for a verbatim column but this one lacks the verbatim prefix!
      return null;
    }

    // this is a verbatim term column
    return TermFactory.instance().findTerm(colName.substring(VERBATIM_TERM_PREFIX.length()));
  }

  public static String column(OccurrenceIssue issue) {
    checkNotNull(issue, "issue can't be null");
    return ISSUE_PREFIX + issue.name();
  }

}
