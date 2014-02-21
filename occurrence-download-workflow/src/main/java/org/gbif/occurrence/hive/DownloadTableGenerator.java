package org.gbif.occurrence.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.persistence.hbase.Columns;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

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
    "LEFT OUTER JOIN interpreted2_%1$s ON (interpreted1_%1$s.occurrenceid = interpreted2_%1$s.occurrenceid) LEFT OUTER JOIN dcterm_%1$s ON (interpreted1_%1$s.occurrenceid = dcterm_%1$s.occurrenceid) LEFT OUTER JOIN dwcterm1_%1$s ON (interpreted1_%1$s.occurrenceid = dwcterm1_%1$s.occurrenceid) LEFT OUTER JOIN dwcterm2_%1$s ON (interpreted1_%1$s.occurrenceid = dwcterm2_%1$s.occurrenceid)";

  private static final String HIVE_CREATE_HDFS_TABLE_FMT = "CREATE TABLE %s (%s) STORED AS RCFILE;";
  private static final String HIVE_DROP_TABLE_FMT = "DROP TABLE IF EXISTS %1$s_%2$s;";
  private static final String HBASE_MAP_FMT = "o:%s";
  private static final String HBASE_KEY_MAPPING = ":key";
  private static final String OCC_ID_COL_DEF = DwcTerm.occurrenceID.simpleName().toLowerCase() + " INT";
  private static final String HIVE_OPTS =
    "SET hive.exec.compress.output=true;SET mapred.max.split.size=256000000;SET mapred.output.compression.type=BLOCK;SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;SET hive.hadoop.supports.splittable.combineinputformat=true;SET hbase.client.scanner.caching=200;SET hive.mapred.reduce.tasks.speculative.execution=false;SET hive.mapred.map.tasks.speculative.execution=false;SET hbase.zookeeper.quorum=c1n8.gbif.org,c1n9.gbif.org,c1n10.gbif.org;\n";
  private static final String INSERT_INFO_HDFS = HIVE_OPTS
    + "INSERT OVERWRITE TABLE %1$s SELECT %2$s FROM interpreted1_%1$s %3$s;";

  /**
   * Generates the COLUMN DATATYPE declaration.
   */
  private Function<Term, String> termColumnDecl = new Function<Term, String>() {

    public String apply(Term term) {
      return TermUtils.getHiveColumn(term) + " " + TermUtils.getHiveType(term);
    }
  };

  /**
   * Generates the COLUMN name of a term.
   */
  private Function<Term, String> termColumnDef = new Function<Term, String>() {

    public String apply(Term term) {
      return TermUtils.getHiveColumn(term);
    }
  };

  /**
   * Generates the HBaseMapping of Term.
   */
  private Function<Term, String> termHBaseMappingDef = new Function<Term, String>() {

    public String apply(Term term) {
      return String.format(HBASE_MAP_FMT, Columns.column(term));
    }
  };

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
      columns.add(String.format(VERBATIM_COL_DEF_FMT, TermUtils.getHiveColumn(term)));
    }
    return columns;
  }

  /**
   * Generates a list that contains the all the verbatim column names.
   */
  private List<String> listVerbatimColumns() {
    List<String> columns = Lists.newArrayList();
    for (Term term : TermUtils.verbatimTerms()) {
      columns.add(String.format(VERBATIM_COL_DEF_FMT, TermUtils.getHiveColumn(term)));
    }
    return columns;
  }

  /**
   * Generates a list that contains the verbatim hbase column mappings of the termClass values.
   */
  private <T extends Term> List<String> listVerbatimColumnsMappings(Class<? extends T> termClass) {
    List<String> columnsMappings = Lists.newArrayList();
    for (T term : Iterables.filter(TermUtils.verbatimTerms(), termClass)) {
      columnsMappings.add(String.format(HBASE_MAP_FMT, Columns.verbatimColumn(term)));
    }
    return columnsMappings;
  }

  /**
   * Returns a String that contains the columns of the HDFS table, concatenated by ,.
   */
  private String listHdfsTableColumns() {
    List<String> columns =
      new ImmutableList.Builder<String>()
        .add(OCC_ID_COL_DEF)
        .addAll(listVerbatimColumns())
        .addAll(processInterpretedTerms(termColumnDecl)).build();
    return COMMA_JOINER.join(columns);
  }


  /**
   * Generates the Hive Create statement to create the interpreted occurrence tables.
   */
  public String buildInterpretedOccurrenceTable(String hiveTableName, String hbaseTableName) {
    return buildSplitTermsTable(processInterpretedTerms(termHBaseMappingDef), processInterpretedTerms(termColumnDecl),
      hiveTableName, hbaseTableName, "interpreted");
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
    return String.format(CREATE_TABLE_FMT, termClass.getSimpleName().toLowerCase(),
      hiveTableName, COMMA_JOINER.join(columns),
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
      hiveTableName, hbaseTableName, "dwcterm");
  }


  /**
   * Generates a Hive INSERT into SELECT from statement. The target table is the HDFS tables and the sources tables are
   * the verbatim and interpreted tables.
   */
  public String generateSelectInto(String hiveTableName) {
    List<String> columns = Lists.newArrayList();
    columns.add(String.format("interpreted1_%s." + DwcTerm.occurrenceID.simpleName().toLowerCase(), hiveTableName));
    for (Term term : TermUtils.verbatimTerms()) {
      columns.add(String.format(VERBATIM_COL_FMT, TermUtils.getHiveColumn(term)));
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


  public static void main(String[] args) throws IOException {
    Closer closer = Closer.create();
    if (args.length < 2) {
      System.err.println("At least 2 parameters are required: the hive output table and the hbase input table");
    }
    final String hiveTableName = args[0];
    final String hbaseTableName = args[1];
    if (args.length > 2) {
      System.setOut(closer.register(new PrintStream(new File(args[3]))));
    } else {
      System.out.println("No output file parameter specified, using the console as output");
    }
    DownloadTableGenerator downloadTableGenerator = new DownloadTableGenerator();
    System.out.println(downloadTableGenerator.buildHdfsTable(hiveTableName));
    System.out.println(downloadTableGenerator.buildVerbatimTermsTable(DcTerm.class, hiveTableName, hbaseTableName));
    System.out.println(downloadTableGenerator.buildDwcTermsTable(hiveTableName, hbaseTableName));
    System.out.println(downloadTableGenerator.buildInterpretedOccurrenceTable(hiveTableName, hbaseTableName));
    System.out.println(downloadTableGenerator.generateSelectInto(hiveTableName));
    System.out.println(downloadTableGenerator.buildDropStatements(hiveTableName));
    closer.close();
  }
}
