package org.gbif.occurrence.persistence.hbase;

/**
 * Constants for commonly used HBase column families, column names, and row ids.
 */
public class TableConstants {



  /**
   * Should never be instantiated.
   */
  private TableConstants() {
  }

  // the one column family for all columns of the occurrence table
  public static final String OCCURRENCE_COLUMN_FAMILY = "o";

  // a prefix required for all non term based columns
  static final String INTERNAL_PREFIX = "_";
  // each UnknownTerm is prefixed differently
  static final String VERBATIM_TERM_PREFIX = "v_";

  // a single occurrence will have 0 or more OccurrenceIssues. Once column per issue, each one prefixed
  static final String ISSUE_PREFIX = INTERNAL_PREFIX + "iss_";

  // An occurrence can have 0-n identifiers, each of a certain type. Their column names look like _t1, _i1, _t2, _i2, etc.
  static final String IDENTIFIER_TYPE_COLUMN = INTERNAL_PREFIX + "t";
  static final String IDENTIFIER_COLUMN = INTERNAL_PREFIX + "i";

  // the counter table is a single cell that is the "autoincrement" number for new keys, with column family, column,
  // and key ("row" in hbase speak)
  public static final String COUNTER_COLUMN = INTERNAL_PREFIX + "id";
  public static final int COUNTER_ROW = 1;

  // the lookup table is a secondary index of unique ids (holy triplet or publisher-provided) to GBIF integer keys
  public static final String LOOKUP_KEY_COLUMN = INTERNAL_PREFIX + "i";
  public static final String LOOKUP_LOCK_COLUMN = INTERNAL_PREFIX + "l";
  public static final String LOOKUP_STATUS_COLUMN = INTERNAL_PREFIX + "s";
}
