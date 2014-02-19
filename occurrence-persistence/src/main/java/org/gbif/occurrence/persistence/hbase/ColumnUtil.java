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
public class ColumnUtil {

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
  private ColumnUtil() {
  }

  /**
   * Returns the column for the given term.
   * If an interpreted column exists for the given term it will be returned, otherwise the verbatim column will be used.
   * Not that InternalTerm are always interpreted and do not exist as verbatim columns.
   *
   * Asking for a "secondary" interpreted term like country which is used during interpretation but not stored
   * will result in an IllegalArgumentException. dwc:countryCode is the right term in this case.
   */
  public static String getColumn(Term term) {
    if (term instanceof InternalTerm || TermUtils.isOccurrenceJavaProperty(term)) {
      return getColumn(term, "");

    } else if(TermUtils.isInterpretedSourceTerm(term)) {
      // "secondary" terms used in interpretation but not used to store the interpreted values should never be asked for
      throw new IllegalArgumentException("The term " + term + " is interpreted and only relevant for verbatim values");

    } else {
      return getVerbatimColumn(term);
    }
  }

  /**
   * Returns the verbatim column for a term.
   * InternalTerm is not permitted and will result in an IllegalArgumentException!
   * @param term
   * @return
   */
  public static String getVerbatimColumn(Term term) {
    if (term instanceof InternalTerm) {
      throw new IllegalArgumentException("Internal terms do not exist as verbatim columns");
    }
    return getColumn(term, TableConstants.VERBATIM_TERM_PREFIX);
  }

  private static String getColumn(Term term, String colPrefix) {
    checkNotNull(term, "term can't be null");

    // unknown terms will never be mapped in Hive, and we can't replace : with anything and guarantee that it will
    // be reversible
    if (term instanceof UnknownTerm) {
      return colPrefix + term.qualifiedName();
    }

    // known terms are mapped to their unique simple name with an optional (v_) prefix
    return colPrefix + term.simpleName();
  }

  public static String getIdColumn(int index) {
    return TableConstants.IDENTIFIER_COLUMN + index;
  }

  public static String getIdTypeColumn(int index) {
    return TableConstants.IDENTIFIER_TYPE_COLUMN+ index;
  }

  /**
   * Returns the term for a strictly verbatim column.
   * If the column given is not a verbatim column, null will be returned.
   */
  @Nullable
  public static Term getTermFromVerbatimColumn(byte[] qualifier) {
    checkNotNull(qualifier, "qualifier can't be null");
    String colName = Bytes.toString(qualifier);

    if (!colName.startsWith(TableConstants.VERBATIM_TERM_PREFIX)) {
      // we asked for a verbatim column but this one lacks the verbatim prefix!
      return null;
    }

    // this is a verbatim term column
    return TermFactory.instance().findTerm(colName.substring(TableConstants.VERBATIM_TERM_PREFIX.length()));
  }

  public static String getColumn(OccurrenceIssue issue) {
    checkNotNull(issue, "issue can't be null");
    return TableConstants.ISSUE_PREFIX + issue.name();
  }

}
