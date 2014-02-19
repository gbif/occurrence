package org.gbif.occurrence.persistence.hbase;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.persistence.api.InternalTerm;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.hbase.util.Bytes;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Utility class to translate from FieldNames to their corresponding HBase column name (in the occurrence table). The
 * first value in the returned list is the field's column family, and the second value is the field's qualifier.
 */
public class FieldNameUtil {

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
  private FieldNameUtil() {
  }

  /**
   * Returns the non verbatim column for the given term
   */
  public static String getColumn(Term term) {
    return getColumn(term, "");
  }

  public static String getVerbatimColumn(Term term) {
    if (term instanceof InternalTerm) {
      throw new IllegalArgumentException("Internal terms cannot exist as verbatim columns");
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

  @Nullable
  public static Term getTermFromVerbatimColumn(@NotNull byte[] qualifier) {
    return getTermFromColumn(qualifier, false);
  }

  @Nullable
  public static Term getTermFromColumn(@NotNull byte[] qualifier) {
    return getTermFromColumn(qualifier, false);
  }

  @Nullable
  private static Term getTermFromColumn(@NotNull byte[] qualifier, boolean verbatim) {
    checkNotNull(qualifier, "qualifier can't be null");
    String colName = Bytes.toString(qualifier);

    if (colName.startsWith(TableConstants.INTERNAL_PREFIX)) {
      // no term based column
      return null;
    }

    if (colName.startsWith(TableConstants.VERBATIM_TERM_PREFIX)) {
      // this is a verbatim term column
      if (!verbatim) {
        //... but we did not ask for one!
        return null;
      }
      colName = colName.substring(TableConstants.VERBATIM_TERM_PREFIX.length());

    } else if(verbatim) {
      // we asked for a verbatim column but this one lacks the verbatim prefix!
      return null;
    }

    return TermFactory.instance().findTerm(colName);
  }

  public static String getColumn(@NotNull OccurrenceIssue issue) {
    checkNotNull(issue, "issue can't be null");
    return TableConstants.ISSUE_PREFIX + issue.name();
  }

}
