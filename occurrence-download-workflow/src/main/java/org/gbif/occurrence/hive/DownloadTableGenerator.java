package org.gbif.occurrence.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Utility class that generates the Hive scripts required to create an HDFS table that is used by occurrence downloads.
 */
public class DownloadTableGenerator {

  private static final String VERBATIM_COL_FMT = "v_%s";
  private static final String VERBATIM_COL_DEF_FMT = VERBATIM_COL_FMT + " STRING";
  private static final Joiner COMMA_JOINER = Joiner.on(',').skipNulls();
  private static final String CREATE_TABLE_FMT =
    "CREATE EXTERNAL TABLE %s_%s (%s) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \"%s\") TBLPROPERTIES(\"hbase.table.name\" = \"%s\",\"hbase.table.default.storage.type\" = \"binary\");";
  private static final String JOIN_FMT =
    " LEFT OUTER JOIN Interpreted2_%1$s ON (Interpreted1_%1$s.occurrenceID = Interpreted2_%1$s.occurrenceID) LEFT OUTER JOIN DcTerm_%1$s ON (Interpreted1_%1$s.occurrenceID = DcTerm_%1$s.occurrenceID) LEFT OUTER JOIN DwcTerm1_%1$s ON (Interpreted1_%1$s.occurrenceID = DwcTerm1_%1$s.occurrenceID) LEFT OUTER JOIN DwcTerm2_%1$s ON (Interpreted1_%1$s.occurrenceID = DwcTerm2_%1$s.occurrenceID)";

  private static final String HIVE_CREATE_HDFS_TABLE_FMT = "CREATE TABLE %s (%s);";
  private static final String HIVE_DROP_TABLE_FMT = "DROP TABLE IF EXISTS %1$s_%2$s;";
  private static final String HBASE_MAP_FMT = "o:%s";
  private static final String HBASE_KEY_MAPPING = ":key";
  private static final String OCC_ID_COL_DEF = DwcTerm.occurrenceID.simpleName() + " INT";

  private static final String INSERT_INFO_HDFS = "INSERT OVERWRITE TABLE %1$s SELECT %2$s FROM Interpreted1_%1$s %3$s;";
  private ImmutableSet<String> HIVE_RESERVED_WORDS = new ImmutableSet.Builder<String>().add("date", "order", "format",
    "group").build();

  /**
   * Generates the COLUMN DATATYPE declaration.
   */
  private Function<Term, String> termColumnDecl = new Function<Term, String>() {

    public String apply(Term term) {
      return getColumnName(term) + " " + getHiveDataType(term);
    }
  };

  /**
   * Generates the COLUMN name of a term.
   */
  private Function<Term, String> termColumnDef = new Function<Term, String>() {

    public String apply(Term term) {
      return getColumnName(term);
    }
  };

  /**
   * Generates the HBaseMapping of Term.
   */
  private Function<Term, String> termHBaseMappingDef = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(HBASE_MAP_FMT, HBaseFieldUtil.getHBaseColumn(term).getColumnName());
    }
  };


  /**
   * Gets the Hive column name of the term parameter.
   */
  private String getColumnName(Term term) {
    if (HIVE_RESERVED_WORDS.contains(term.simpleName().toLowerCase())) {
      return term.simpleName() + '_';
    }
    return term.simpleName();
  }

  /**
   * Applies the transformer to the list of interpreted terms (excluding DwcTerm.occurrenceID).
   * Return a list of strings that contains the transformations of each term.
   */
  private List<String> processInterpretedTerms(Function<Term, String> transformer) {
    List<String> interpretedColumns = Lists.newArrayList();
    for (Term term : TermUtils.interpretedTerms()) {
      if (DwcTerm.occurrenceID != term) {
        interpretedColumns.add(transformer.apply(term));
      }
    }
    return interpretedColumns;
  }


  /**
   * Generates a list that contains the verbatim column names of the termClass values.
   */
  private <T extends Term> List<String> listVerbatimColumns(Class<? extends T> termClass) {
    List<String> columns = Lists.newArrayList();
    for (T term : Iterables.filter(TermUtils.verbatimTerms(), termClass)) {
      columns.add(String.format(VERBATIM_COL_DEF_FMT, getColumnName(term)));
    }
    return columns;
  }

  /**
   * Generates a list that contains the verbatim hbase column mappings of the termClass values.
   */
  private <T extends Term> List<String> listVerbatimColumnsMappings(Class<? extends T> termClass) {
    List<String> columnsMappings = Lists.newArrayList();
    for (T term : Iterables.filter(TermUtils.verbatimTerms(), termClass)) {
      columnsMappings.add(String.format(HBASE_MAP_FMT, HBaseFieldUtil.getHBaseColumn(term).getColumnName()));
    }
    return columnsMappings;
  }

  /**
   * Returns the Hive data type of term parameter.
   */
  private String getHiveDataType(Term term) {
    if (TermUtils.isInterpretedNumerical(term)) {
      return "INT";
    } else if (TermUtils.isInterpretedDate(term)) {
      return "BIGINT";
    } else {
      return "STRING";
    }
  }

  /**
   * Returns a String that contains the columns of the HDFS table, concatenated by ,.
   */
  private String listHdfsTableColumns() {
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(OCC_ID_COL_DEF)
        .addAll(listVerbatimColumns(DcTerm.class)).addAll(listVerbatimColumns(DwcTerm.class))
        .addAll(processInterpretedTerms(termColumnDecl)).build();
    return COMMA_JOINER.join(columns);
  }


  /**
   * Generates the Hive Create statement to create the interpreted occurrence tables.
   */
  public String buildInterpretedOccurrenceTable(String hiveTableName, String hbaseTableName) {
    return buildSplitTermsTable(processInterpretedTerms(termHBaseMappingDef), processInterpretedTerms(termColumnDecl),
      hiveTableName, hbaseTableName, "Interpreted");
  }


  /**
   * Generates the Hive CREATE statement to create a verbatim table of a Term.
   */
  private <T extends Term> String buildVerbatimTermsTable(Class<? extends T> termClass, String hiveTableName,
    String hbaseTableName) {
    List<String> columns = Lists.newArrayList();
    columns.add(OCC_ID_COL_DEF);
    columns.addAll(listVerbatimColumns(termClass));
    List<String> columnsMappings = Lists.newArrayList();
    columnsMappings.add(HBASE_KEY_MAPPING);
    columnsMappings.addAll(listVerbatimColumnsMappings(termClass));
    return String.format(CREATE_TABLE_FMT, termClass.getSimpleName(), hiveTableName, COMMA_JOINER.join(columns),
      COMMA_JOINER.join(columnsMappings), hbaseTableName);
  }

  /**
   * Generates the Hive CREATE statement for 2 tables that will contain the columns split in 2 sets of the columns
   * parameters.
   * Each table will be named tablePrefix + (1|2) + hiveTableName.
   */
  private String buildSplitTermsTable(List<String> columnsMappings, List<String> columns, String hiveTableName,
    String hbaseTableName, String tablePrefix) {
    int half = columns.size() / 2;
    List<String> termsColumns1 = Lists.newArrayList(columns.subList(0, half));
    termsColumns1.add(0, OCC_ID_COL_DEF);
    List<String> termsColumns2 = Lists.newArrayList(columns.subList(half, columns.size()));
    termsColumns2.add(0, OCC_ID_COL_DEF);

    List<String> columnsMappings1 = Lists.newArrayList(columnsMappings.subList(0, half));
    columnsMappings1.add(0, HBASE_KEY_MAPPING);
    List<String> columnsMappings2 = Lists.newArrayList(columnsMappings.subList(half, columnsMappings.size()));
    columnsMappings2.add(0, HBASE_KEY_MAPPING);

    return String.format(CREATE_TABLE_FMT, tablePrefix + '1', hiveTableName, COMMA_JOINER.join(termsColumns1),
      COMMA_JOINER.join(columnsMappings1), hbaseTableName) + '\n' +
      String.format(CREATE_TABLE_FMT, tablePrefix + '2', hiveTableName, COMMA_JOINER.join(termsColumns2),
        COMMA_JOINER.join(columnsMappings2), hbaseTableName);
  }

  /**
   * Generates the Hive CREATE statement of 2 tables that contains the DwcTerms.
   */
  public String buildDwcTermsTable(String hiveTableName, String hbaseTableName) {
    return buildSplitTermsTable(listVerbatimColumnsMappings(DwcTerm.class), listVerbatimColumns(DwcTerm.class),
      hiveTableName, hbaseTableName, "DwcTerm");
  }


  /**
   * Generates a Hive INSERT into SELECT from statement. The target table is the HDFS tables and the sources tables are
   * the verbatim and interpreted tables.
   */
  public String generateSelectInto(String hiveTableName) {
    List<String> columns = Lists.newArrayList();
    columns.add(String.format("Interpreted1_%s." + DwcTerm.occurrenceID.simpleName(), hiveTableName));
    for (Term term : TermUtils.verbatimTerms()) {
      columns.add(String.format(VERBATIM_COL_FMT, getColumnName(term)));
    }

    columns.addAll(processInterpretedTerms(termColumnDef));

    return String.format(INSERT_INFO_HDFS, hiveTableName, COMMA_JOINER.join(columns),
      String.format(JOIN_FMT, hiveTableName));
  }

  /**
   * Generates the CREATE statement of the occurrence HDFS table.
   */
  public String buildHdfsTable(String hiveTableName) {
    return String.format(HIVE_CREATE_HDFS_TABLE_FMT, hiveTableName, listHdfsTableColumns());
  }

  /**
   * Generates the DROP statements for the intermediate tables.
   */
  public String buildDropStatements(String hiveTableName) {
    return new StringBuilder().append(String.format(HIVE_DROP_TABLE_FMT, "dcterm", hiveTableName))
      .append('\n')
      .append(String.format(HIVE_DROP_TABLE_FMT, "dwcterm1", hiveTableName))
      .append('\n')
      .append(String.format(HIVE_DROP_TABLE_FMT, "dwcterm2", hiveTableName))
      .append('\n')
      .append(String.format(HIVE_DROP_TABLE_FMT, "interpreted1", hiveTableName))
      .append('\n')
      .append(String.format(HIVE_DROP_TABLE_FMT, "interpreted2", hiveTableName))
      .toString();

  }


  public static void main(String[] args) {
    final String hiveTableName = args[0];
    final String hbaseTableName = args[1];
    DownloadTableGenerator downloadTableGenerator = new DownloadTableGenerator();
    System.out.println(downloadTableGenerator.buildHdfsTable(hiveTableName));
    System.out.println(downloadTableGenerator.buildVerbatimTermsTable(DcTerm.class, hiveTableName, hbaseTableName));
    System.out.println(downloadTableGenerator.buildDwcTermsTable(hiveTableName, hbaseTableName));
    System.out.println(downloadTableGenerator.buildInterpretedOccurrenceTable(hiveTableName, hbaseTableName));
    System.out.println(downloadTableGenerator.generateSelectInto(hiveTableName));
    System.out.println(downloadTableGenerator.buildDropStatements(hiveTableName));
  }
}
